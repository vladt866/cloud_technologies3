/*using SPS.Abstractions;

namespace SPS.Student;

public sealed class MyStudentPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new MyRequestPolicy();

    private readonly Dictionary<string, long> _hitCounts = new();
    private readonly Dictionary<long, int> _endpointFailures = new();
    private readonly Dictionary<long, TimeSpan> _instanceReadyAt = new();

    private double _targetWorkers = 2.0;

    // Минимальный возраст Ready-воркера до retire — защита от 0-second churn.
    private static readonly TimeSpan MinReadyAge = TimeSpan.FromSeconds(2);

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random) => null;

    // ============================================================
    //  Маршрутизация API
    // ============================================================
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;
        var healthy = context.Endpoints
            .Where(e => _endpointFailures.GetValueOrDefault(e.Id) < 3)
            .ToArray();
        if (healthy.Length == 0) healthy = context.Endpoints.ToArray();
        var best = healthy
            .OrderBy(e => (double)e.OutstandingCalls / Math.Max(1, e.Slots))
            .ThenByDescending(e => e.Speed)
            .ThenBy(e => e.Id)
            .First();
        return EndpointSelectionDecision.Assign(best.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var e = context.Endpoint;
        if (e.ProbeOutstanding) return EndpointDecision.Allow();
        if (e.LastHeartbeat is { } hb && context.Now - hb > TimeSpan.FromSeconds(5))
            return EndpointDecision.Block();
        var recent = context.RecentResults;
        if (recent.Count >= 3 &&
            recent.Skip(recent.Count - 3).All(r => r.Outcome == RequestOutcome.Timeout))
            return EndpointDecision.Probe();
        return EndpointDecision.Allow();
    }

    // ============================================================
    //  Кэш
    // ============================================================
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var safety = TimeSpan.FromMilliseconds(20);
        if (context.Entry is { } entry
            && (context.Now - entry.SourceObservedAt) < context.Request.MaxDataAge - safety)
        {
            _hitCounts[entry.Key] = _hitCounts.GetValueOrDefault(entry.Key) + 1;
            return CacheReadDecision.ServeCached();
        }
        var pending = context.PendingFetches
            .Where(f => f.Key == context.Request.ResourceKey)
            .OrderBy(f => f.StartedAt)
            .FirstOrDefault();
        if (pending != null) return CacheReadDecision.JoinFetch(pending.Id);
        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var v = context.Value;
        if (v.SizeBytes > context.CapacityBytes) return CacheWriteDecision.Skip();

        bool isPreview = v.Key.StartsWith("Preview:", StringComparison.Ordinal);
        TimeSpan ttl = isPreview ? TimeSpan.FromSeconds(5) : TimeSpan.FromSeconds(2);

        long available = context.CapacityBytes
            - context.Entries.Where(e => e.Key != v.Key).Sum(e => e.SizeBytes);

        var evictions = new List<string>();
        if (available < v.SizeBytes)
        {
            var victims = context.Entries
                .Where(e => e.Key != v.Key)
                .OrderBy(e => _hitCounts.GetValueOrDefault(e.Key))
                .ThenBy(e => e.SourceObservedAt)
                .ToArray();
            foreach (var victim in victims)
            {
                evictions.Add(victim.Key);
                available += victim.SizeBytes;
                if (available >= v.SizeBytes) break;
            }
            if (available < v.SizeBytes) return CacheWriteDecision.Skip();
        }
        foreach (var key in evictions) _hitCounts.Remove(key);
        return CacheWriteDecision.Retain(ttl, evictions.ToArray());
    }

    // ============================================================
    //  Распределение заданий
    // ============================================================
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        var candidates = context.QueuedJobs.Select(j =>
        {
            double age = (context.Now - j.Request.CreatedAt).TotalSeconds;
            double work = Math.Max(0.5, j.Request.EstimatedWorkSeconds);
            double pSuccess = Math.Max(0.0, Math.Min(1.0, (35.0 - age - work) / 20.0));
            double ev = pSuccess * (double)j.Request.Revenue;
            double rate = ev / work;
            return (Job: j, EV: ev, Rate: rate, Age: age);
        }).ToArray();

        var worst = candidates.OrderBy(c => c.EV).First();
        if (worst.EV < 0.15) return JobAssignmentDecision.Reject(worst.Job.Id);

        var best = candidates
            .OrderByDescending(c => c.Rate)
            .ThenBy(c => c.Age)
            .First();

        var worker = context.AvailableWorkers
            .OrderByDescending(w => w.Speed)
            .ThenBy(w => w.AssignedJobs)
            .ThenBy(w => w.Id)
            .First();
        return JobAssignmentDecision.Assign(best.Job.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        if (job.AssignedAt == null) return JobRecoveryDecision.KeepWaiting();
        var lastSignal = job.LastHeartbeat ?? job.AssignedAt.Value;
        if (context.Now - lastSignal > TimeSpan.FromSeconds(2))
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(200));
        return JobRecoveryDecision.KeepWaiting();
    }

    // ============================================================
    //  Масштабирование
    // ============================================================
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var s = context.Settings;
        int ready = context.Instances.Count(i => i.Role == InstanceRole.Worker && i.State == InstanceState.Ready);
        int busy = context.Instances.Where(i => i.Role == InstanceRole.Worker).Sum(i => i.AssignedJobs);
        int queue = context.QueuedJobs.Count;

        double demand = (busy + queue) / (double)Math.Max(1, s.Worker.Slots);

        // Асимметричный EMA:
        //  - рост быстро (0.15) — не теряем пики;
        //  - спад очень быстро (0.30) при пустой очереди — фаза 5;
        //  - иначе спад медленно (0.05) — защита от транзиентных провалов.
        double alpha;
        if (demand > _targetWorkers) alpha = 0.15;
        else if (queue == 0) alpha = 0.30;
        else alpha = 0.05;

        _targetWorkers = (1 - alpha) * _targetWorkers + alpha * demand;
        _targetWorkers = Math.Clamp(_targetWorkers, 2.0, s.MaximumWorkers);

        int desiredWorkers = (int)Math.Ceiling(_targetWorkers);

        // Growth gate: спавним ТОЛЬКО при queue >= 3.
        // В фазе 1 (0.6 gen/s) 2 воркера справляются, queue редко доходит до 3 —
        // спекулятивных спавнов не будет. В фазе 2 queue достигает 3 за 2 с — реакция быстрая.
        if (desiredWorkers > ready && queue < 3)
            desiredWorkers = Math.Max(2, ready);

        int maxByCompute = (s.ComputeUnitLimit - 2 * s.Api.ComputeUnits) / s.Worker.ComputeUnits;
        desiredWorkers = Math.Min(desiredWorkers, Math.Max(2, maxByCompute));
        desiredWorkers = Math.Min(desiredWorkers, s.MaximumWorkers);

        const int desiredApis = 2;

        // Retire: только idle + mature (>= 2s после Ready). Newest-first.
        List<long>? retire = null;
        if (ready > desiredWorkers)
        {
            var idle = context.Instances
                .Where(i => i.Role == InstanceRole.Worker
                         && i.State == InstanceState.Ready
                         && i.AssignedJobs == 0
                         && _instanceReadyAt.GetValueOrDefault(i.Id, TimeSpan.Zero) <= context.Now - MinReadyAge)
                .OrderByDescending(i => i.Id)
                .Take(ready - desiredWorkers)
                .Select(i => i.Id)
                .ToList();
            if (idle.Count > 0) retire = idle;
        }

        return new CapacityPlan(desiredApis, desiredWorkers, retire);
    }

    // ============================================================
    //  Наблюдения
    // ============================================================
    public void Observe(SystemObservation observation)
    {
        switch (observation.Kind)
        {
            case ObservationKind.RpcAttemptCompleted when observation.EndpointId is { } eid:
                if (observation.Outcome == RequestOutcome.Timeout)
                    _endpointFailures[eid] = _endpointFailures.GetValueOrDefault(eid) + 1;
                else if (observation.Outcome == RequestOutcome.Success)
                    _endpointFailures[eid] = 0;
                break;
            case ObservationKind.CacheEntryRemoved when observation.Key is { } k:
                _hitCounts.Remove(k);
                break;
            case ObservationKind.InstanceReady when observation.EndpointId is { } rid:
                _instanceReadyAt[rid] = observation.Now;
                break;
            case ObservationKind.InstanceStopped when observation.EndpointId is { } sid:
                _instanceReadyAt.Remove(sid);
                break;
        }
    }

    private sealed class MyRequestPolicy : IRequestPolicy
    {
        public TimeSpan GetTimeout(RequestContext context, Random random)
            => TimeSpan.FromMilliseconds(250);

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (observation.Outcome is RequestOutcome.Success
                or RequestOutcome.Rejected
                or RequestOutcome.PolicyRejected
                or RequestOutcome.Stale)
                return RetryDecision.NoRetry();
            if (observation.Attempt >= 3) return RetryDecision.NoRetry();
            return observation.Outcome switch
            {
                RequestOutcome.Timeout => RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(40)),
                RequestOutcome.Unavailable => RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(120)),
                _ => RetryDecision.NoRetry()
            };
        }
    }
}
*/
using SPS.Abstractions;

namespace SPS.Student;

// Стратегия оптимизирует Profit = Revenue − ApiCost − WorkerCost.

// Поэтому основные рычаги: выбор задачи воркеру и масштабирование без churn.
public sealed class MyStudentPolicy : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new MyRequestPolicy();

    // Счётчик попаданий по ключу — для LFU-эвикции кэша.
    private readonly Dictionary<string, long> _hitCounts = new();

    // Подряд идущие Timeout'ы на эндпоинт — чтобы временно его исключать.
    private readonly Dictionary<long, int> _endpointFailures = new();

    //  для каждого instance ID — момент, когда он перешёл в состояние Ready.
    private readonly Dictionary<long, TimeSpan> _instanceReadyAt = new();

    // Сглаженная цель по числу воркеров.
    private double _targetWorkers = 2.0;

    // Не retire'им воркер, пока он не прожил 2 с после Ready (защита от churn).
    private static readonly TimeSpan MinReadyAge = TimeSpan.FromSeconds(2);

    // Свой лимит времени не ставим — клиент сам уйдёт по patience.
    //При RequestArrived движок вызывает этот метод и спрашивает: «Сколько максимального времени ты готов обрабатывать этот запрос?»
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random) => null;

    // Выбор API. Metadata сюда не попадает (идёт сразу в storage).
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        if (context.Endpoints.Count == 0) return null;

        // Сначала отбрасываем «больные» эндпоинты (3+ подряд Timeout'а).
        var healthy = context.Endpoints
            .Where(e => _endpointFailures.GetValueOrDefault(e.Id) < 3)
            .ToArray();
        if (healthy.Length == 0) healthy = context.Endpoints.ToArray();

        // Берём наименее загруженный; при равенстве — более быстрый.
        var best = healthy
            .OrderBy(e => (double)e.OutstandingCalls / Math.Max(1, e.Slots))
            .ThenByDescending(e => e.Speed)
            .ThenBy(e => e.Id)
            .First();

        return EndpointSelectionDecision.Assign(best.Id);
    }

    // Проверка здоровья эндпоинта. Block — только по пропавшему heartbeat.
    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var e = context.Endpoint;

        // Уже проверяем Probe'ом — не дублируем.
        if (e.ProbeOutstanding) return EndpointDecision.Allow();

        // Heartbeat молчит 5 с — считаем мёртвым.
        if (e.LastHeartbeat is { } hb && context.Now - hb > TimeSpan.FromSeconds(5))
            return EndpointDecision.Block();

        // Три Timeout'а подряд — мягкая проверка Probe'ом.
        var recent = context.RecentResults;
        if (recent.Count >= 3 &&
            recent.Skip(recent.Count - 3).All(r => r.Outcome == RequestOutcome.Timeout))
            return EndpointDecision.Probe();

        return EndpointDecision.Allow();
    }

    // Чтение кэша: ServeCached → JoinFetch → StartFetch по приоритету.
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        // Запас, чтобы не отдать запись, которая «протухнет» по пути к клиенту.
        var safety = TimeSpan.FromMilliseconds(20);

        // Отдаём из кэша, только если запись свежая для MaxDataAge этого клиента.
        if (context.Entry is { } entry
            && (context.Now - entry.SourceObservedAt) < context.Request.MaxDataAge - safety)
        {
            _hitCounts[entry.Key] = _hitCounts.GetValueOrDefault(entry.Key) + 1;
            return CacheReadDecision.ServeCached();
        }

        // Если уже есть fetch по этому ключу — присоединяемся бесплатно.
        var pending = context.PendingFetches
            .Where(f => f.Key == context.Request.ResourceKey)
            .OrderBy(f => f.StartedAt)
            .FirstOrDefault();
        if (pending != null) return CacheReadDecision.JoinFetch(pending.Id);

        // Иначе запускаем новый fetch.
        return CacheReadDecision.StartFetch();
    }

    // Запись кэша. TTL = MaxDataAge профиля, эвикция по LFU.
    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var v = context.Value;
        if (v.SizeBytes > context.CapacityBytes) return CacheWriteDecision.Skip();

        // TTL совпадает с допустимой свежестью клиента.
        bool isPreview = v.Key.StartsWith("Preview:", StringComparison.Ordinal);
        TimeSpan ttl = isPreview ? TimeSpan.FromSeconds(5) : TimeSpan.FromSeconds(2);

        // Сколько места останется, если перезаписать текущий ключ.
        long available = context.CapacityBytes
            - context.Entries.Where(e => e.Key != v.Key).Sum(e => e.SizeBytes);

        var evictions = new List<string>();
        if (available < v.SizeBytes)
        {
            // LFU: сначала холодные ключи, при равенстве — самые старые.
            var victims = context.Entries
                .Where(e => e.Key != v.Key)
                .OrderBy(e => _hitCounts.GetValueOrDefault(e.Key))
                .ThenBy(e => e.SourceObservedAt)
                .ToArray();

            foreach (var victim in victims)
            {
                evictions.Add(victim.Key);
                available += victim.SizeBytes;
                if (available >= v.SizeBytes) break;
            }
            if (available < v.SizeBytes) return CacheWriteDecision.Skip();
        }

        foreach (var key in evictions) _hitCounts.Remove(key);
        return CacheWriteDecision.Retain(ttl, evictions.ToArray());
    }

    // Распределение заданий — главный рычаг по Profit.
    // Считаем pSuccess по возрасту и work, сортируем по EV/work.
    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        // pSuccess = clamp((35 − age − work) / 20, 0, 1) — под patience U[15, 35].
        var candidates = context.QueuedJobs.Select(j =>
        {
            double age = (context.Now - j.Request.CreatedAt).TotalSeconds;
            double work = Math.Max(0.5, j.Request.EstimatedWorkSeconds);
            double pSuccess = Math.Max(0.0, Math.Min(1.0, (35.0 - age - work) / 20.0));
            double ev = pSuccess * (double)j.Request.Revenue;
            double rate = ev / work;
            return (Job: j, EV: ev, Rate: rate, Age: age);
        }).ToArray();

        // Совсем безнадёжные (EV < 0.15) — сразу reject, чтобы не занимали очередь.
        var worst = candidates.OrderBy(c => c.EV).First();
        if (worst.EV < 0.15) return JobAssignmentDecision.Reject(worst.Job.Id);

        // Лучший по выручке на секунду воркера; при равенстве — старший.
        var best = candidates
            .OrderByDescending(c => c.Rate)
            .ThenBy(c => c.Age)
            .First();

        // Воркер: самый быстрый, потом менее загруженный.
        var worker = context.AvailableWorkers
            .OrderByDescending(w => w.Speed)
            .ThenBy(w => w.AssignedJobs)
            .ThenBy(w => w.Id)
            .First();

        return JobAssignmentDecision.Assign(best.Job.Id, worker.Id);
    }

    // Решение о переотдаче зависшего задания. 2 с без heartbeat = 4 пропущенных удара.
    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;
        if (job.AssignedAt == null) return JobRecoveryDecision.KeepWaiting();

        var lastSignal = job.LastHeartbeat ?? job.AssignedAt.Value;
        if (context.Now - lastSignal > TimeSpan.FromSeconds(2))
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromMilliseconds(200));

        return JobRecoveryDecision.KeepWaiting();
    }

    // Масштабирование воркеров: сглаженная цель + защита от churn.
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var s = context.Settings;
        int ready = context.Instances.Count(i => i.Role == InstanceRole.Worker && i.State == InstanceState.Ready);
        int busy = context.Instances.Where(i => i.Role == InstanceRole.Worker).Sum(i => i.AssignedJobs);
        int queue = context.QueuedJobs.Count;

        // Сколько воркеров нужно прямо сейчас.
        double demand = (busy + queue) / (double)Math.Max(1, s.Worker.Slots);

        // EMA: быстро вверх, очень быстро вниз при пустой очереди, медленно иначе.
        double alpha;
        if (demand > _targetWorkers) alpha = 0.15;
        else if (queue == 0) alpha = 0.30;
        else alpha = 0.05;

        _targetWorkers = (1 - alpha) * _targetWorkers + alpha * demand;
        _targetWorkers = Math.Clamp(_targetWorkers, 2.0, s.MaximumWorkers);

        int desiredWorkers = (int)Math.Ceiling(_targetWorkers);

        // Без реальной очереди (queue < 3) не растим — защита от спекулятивных спавнов.
        if (desiredWorkers > ready && queue < 3)
            desiredWorkers = Math.Max(2, ready);

        // Жёсткий предел по ComputeUnitLimit (12): 2 Api + 5 Worker.
        int maxByCompute = (s.ComputeUnitLimit - 2 * s.Api.ComputeUnits) / s.Worker.ComputeUnits;
        desiredWorkers = Math.Min(desiredWorkers, Math.Max(2, maxByCompute));
        desiredWorkers = Math.Min(desiredWorkers, s.MaximumWorkers);

        const int desiredApis = 2;

        // Retire: только idle и «взрослые»; первыми уходят новые (не initial).
        List<long>? retire = null;
        if (ready > desiredWorkers)
        {
            var idle = context.Instances
                .Where(i => i.Role == InstanceRole.Worker
                         && i.State == InstanceState.Ready
                         && i.AssignedJobs == 0
                         && _instanceReadyAt.GetValueOrDefault(i.Id, TimeSpan.Zero) <= context.Now - MinReadyAge)
                .OrderByDescending(i => i.Id)
                .Take(ready - desiredWorkers)
                .Select(i => i.Id)
                .ToList();
            if (idle.Count > 0) retire = idle;
        }

        return new CapacityPlan(desiredApis, desiredWorkers, retire);
    }

    // Копим статистику для решений выше.
    public void Observe(SystemObservation observation)
    {
        switch (observation.Kind)
        {
            // Обновляем счётчик Timeout'ов на эндпоинт.
            case ObservationKind.RpcAttemptCompleted when observation.EndpointId is { } eid:
                if (observation.Outcome == RequestOutcome.Timeout)
                    _endpointFailures[eid] = _endpointFailures.GetValueOrDefault(eid) + 1;
                else if (observation.Outcome == RequestOutcome.Success)
                    _endpointFailures[eid] = 0;
                break;

            // Удалённый ключ больше не нужен в статистике.
            case ObservationKind.CacheEntryRemoved when observation.Key is { } k:
                _hitCounts.Remove(k);
                break;

            // Запоминаем момент Ready.
            case ObservationKind.InstanceReady when observation.EndpointId is { } rid:
                _instanceReadyAt[rid] = observation.Now;
                break;

            // При остановке чистим запись.
            case ObservationKind.InstanceStopped when observation.EndpointId is { } sid:
                _instanceReadyAt.Remove(sid);
                break;
        }
    }

    // Timeout и retry для отдельных RPC.
    private sealed class MyRequestPolicy : IRequestPolicy
    {
        // 250 мс хватает на сеть + API + storage с запасом.
        public TimeSpan GetTimeout(RequestContext context, Random random)
            => TimeSpan.FromMilliseconds(250);

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            // Эти исходы повторять бессмысленно.
            if (observation.Outcome is RequestOutcome.Success
                or RequestOutcome.Rejected
                or RequestOutcome.PolicyRejected
                or RequestOutcome.Stale)
                return RetryDecision.NoRetry();

            // Максимум 3 попытки.
            if (observation.Attempt >= 3) return RetryDecision.NoRetry();

            // Timeout — быстрый retry; Unavailable — с задержкой.
            return observation.Outcome switch
            {
                RequestOutcome.Timeout => RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(40)),
                RequestOutcome.Unavailable => RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(120)),
                _ => RetryDecision.NoRetry()
            };
        }
    }
}