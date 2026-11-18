# database-lock-manager
Python 数据库锁管理器，处理共享锁、排他锁、等待图、死锁和公平队列

## 功能

- 共享锁（S）/ 排他锁（X），支持 S → X 锁升级与可重入请求
- 每个资源一条 FIFO 等待队列，释放锁时唤醒队首最大兼容组（连续的 S waiter 一起授予）
- wait-for graph 环检测，按稳定规则选择 victim 并抛出 `DeadlockError`
- `lock(..., timeout=)` 阻塞等待与超时（`LockTimeoutError`）
- `cancel_wait` 取消排队请求，`release_all` 事务回滚释放全部锁与排队请求
- `try_lock` 非阻塞核心：授予返回 `True`，入队返回 `False`，便于确定性测试

```python
from lock_manager import LockManager, LockMode

lm = LockManager()
lm.lock("txn-1", "row:42", LockMode.EXCLUSIVE, timeout=1.0)
lm.unlock("txn-1", "row:42")
```

运行测试：`python -m unittest`

## 公平性

新请求只有在"与所有持有者兼容"且"队列中没有排在前面的冲突 waiter"时才立即授予。
因此读事务不会插到等待中的写事务前面——写者不会饿死；队首的兼容组会被一起唤醒，
读者也不会被无关的写请求无限阻塞。

## Victim 策略

每次请求入队后重建 wait-for graph（等待者 → 冲突持有者 + 队列中排在前面的冲突
等待者），DFS 找环。发现环时选择**环上最年轻的事务**（注册序号最大者）作为 victim：

- 注册序号在事务首次请求锁时分配，之后不变，因此选择规则稳定、可复现，与请求
  到达的时序无关；
- 撤销 victim 的排队请求即断开环，随后重新检测，直到图中无环（可处理多个环）；
- victim 已持有的锁不强制释放——`DeadlockError` 抛给 victim，由事务层调用
  `release_all` 回滚。环中其余事务继续等待，直到 victim 回滚后被唤醒。

## 隔离边界

本模块只是锁管理核心，**不提供**完整的事务隔离语义：

- 锁粒度是命名资源整体；没有谓词锁 / 间隙锁，不能防幻读；
- 不强制两阶段锁（2PL）：事务可以提前 `unlock` 再继续申请，调度方需自行遵守
  strict 2PL（持有到提交/回滚）才能获得可串行化保证；
- 不做 MVCC、不做锁降级，不跟踪读写集；
- 保证的不变量只有一条：**同一资源上不会同时授予互相冲突的锁**
  （X 与任何锁互斥，S 之间兼容），测试中的 `assert_no_conflict` 与随机模拟
  会持续校验该不变量。
