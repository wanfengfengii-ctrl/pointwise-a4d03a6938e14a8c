# 偏序差分数仓基线

业务：采样金额流 L 与站点归属流 R 各自推进多个采集/重算进度轴。时间不是墙钟，也不是字典序。历史压缩用于长期运行；压缩前后，仍被允许读取的账必须相同，后来合法追加也须继续正确。基线已实现业务，本轮不新增业务能力。

## 入口与文件
生产入口为 lakeflow.Store(db, dimensions=2)，db 为 SQLite 文件路径或 ':memory:'；dimensions 是 2～6 的整数，重开须一致。每个并发调用者使用独立 Store 连接；不要求跨线程共享连接。apply(request, hook=...)、read(time)、inspect()、close() 为公开入口。inspect 返回同一事务快照的 state 与 receipts；格式见源码，允许测试读取，不允许测试修改业务数据库。基线样例在 samples.py；python3 checks.py normal 核对业务，python3 checks.py demo 输出真实结果。

## 时间与进度
时间为固定维数的非负整数数组，布尔值不算整数，坐标不设数值上限。a≤b 当且仅当所有分量 a[j]≤b[j]。前沿是时间数组构成的反链：任意两个元素不可比较，重复元素也非法；存储时字典序排序只用于规范展示。
记 F≤G 为 G 的每个元素都大于等于 F 的某个元素。空前沿表示端口永久关闭：任意 F≤[] 成立，[]≤非空不成立。
每端口有 upper，初始为全零单点；since 为非空压缩前沿，初始亦为全零单点。upper 只能按上述关系前进；since 也只能前进，且必须满足 since≤新 upper_L 和 since≤新 upper_R。

## 批次
request 是恰含 request、expected、changes、upper、since 五个字段的标准 JSON 对象。request 为非空字符串；expected 为非负整数；changes 为数组；upper 是只含 L/R 可选键的对象，未给的端口保持原前沿；since=null 保持原前沿，否则是非空反链。
change=[port,time,row,weight]；port 为 L 或 R；L 的 row=[非空字符串key,整数amount]，R 的 row=[非空字符串key,字符串region]；amount 可为零或负数，region 可为空；weight 为非零整数，可正可负。重复行、重复时间与负的当前累计权重均合法。不同记录不隐式去重。
本批每条 change 的时间须大于等于该端口批前 upper 的某个元素；不能借本批前沿倒退接纳迟到项，端口关闭后不能再写。批内变更与进度一并发布，不能看见批中间态。
每个首次成功请求，包括空 changes 且进度不变的请求，使 generation 恰增一；初始为零。返回 {"request":请求号,"generation":本次代次}。
身份内容是 changes、upper、since 对象经 encode 得到的规范 JSON；对象键顺序无关，数组顺序有关，不包括 expected。重复请求先核对身份：同内容返回第一次回执，即使 expected 已旧；异内容报 REQUEST_CONFLICT。然后才比较 expected 与当前代次，不同报 GENERATION。
请求信封校验先于回执；新请求依次检查 expected、前沿、since、各 change。拒绝不增加代次，不留下任何局部状态或回执。原始规范载荷随回执保存。
strict_load 拒绝非标准 JSON 数值；本题不要求接收任意 Python 活对象。

## 业务不变量
对任一截点 q，L(key,amount) 的重数是所有 time≤q 的该行 weight 之和，R 同理。同 key 的左右行按多重集关联，关联重数为两重数之积；region 的 count 为关联重数和，total 为 amount 乘关联重数之和。Python 整数精确计算，count=0 但 total≠0 的分组仍须保留。
read(q) 先检查 since 中至少一个元素≤q，否则报 COMPACTED；再检查两个 upper 中均不存在元素≤q，否则报 INCOMPLETE；这两条同时成立才是合法查询截点。空 upper 不阻止读取。
成功返回 {"generation":当前快照代次,"rows":[[region,count,total],...]}；rows 按 region 排序，仅省略 count 和 total 同为零的分组。前沿、数据与 generation 必须来自同一快照。
压缩必须保留 since 允许的全部截点上每个端口每行的前缀权重，包括当前尚不完备、将来推进后才能读取的截点；不能只保留当前关联输出。生产实现把 t 推到各 t∨f 的逐分量下确界，再合并同端口、同时间、同行权重；权重和为零则删除。这是既有实现说明，不是测试判定器的可调用算法。
state、压缩迹、两路进度、generation 与首次回执必须一起提交或回滚。读和提交须能解释成遵守调用完成/开始实时先后的原子序列；未知结果的中断请求可提交或不提交，重开后的状态及回执须与其中一种一致。重放历史回执不得回退当前代次、迹或前沿。

## 故障可观察性
apply 的 hook(point) 在 after_begin、after_state、after_receipt、after_commit 被调用；前三处在写事务内，after_commit 在提交之后。重复请求不调用 after_commit。回调异常向调用者传播，提交前异常回滚，提交后异常不撤销已提交数据。可用进程间屏障等待钩子，再由父进程真正终止子进程；不以抛异常代替杀进程。SQLite 负责原子落盘，测试仍须核对业务状态、回执和读进度的联合结果，不把 SQLite 自身测试作为本题交付。

## 样例预期
initial 后 read([1,0]) 为 east/2/20，read([2,2]) 为 east/4/70。
compact 将两路 upper 推到 [[3,3]]、since 推到 [[1,1]]；[2,2] 仍为 east/4/70，read([0,0]) 报 COMPACTED。
correction 后 [2,2] 仍为 east/4/70，[3,3] 为 west/1/25；重放 initial 返回 generation=1，数据库仍为 generation=3。
上述样例展示非全序前沿、真正发生的时间压缩、符号撤回和历史回执。

## 本轮质量产物
保留 lakeflow.py、samples.py 与 checks.py 的原有内容，不通过修复生产源码消除偏差。交付 qa.py、REPORT.md、evidence.json，新增测试辅助文件可自定。
qa.py verify 重跑原断言及新增质量验证，重算并核对报告证据；发现未解决业务偏差须非零退出并保留失败材料。qa.py demo 实际展示压缩前后、重开及旧请求重放的结果。将运行清单 verify/demo 指向这两个入口。
独立判定器只使用公开输入、inspect 返回数据与自行实现的语义，不导入或复制生产前沿、压缩、聚合逻辑。提供 qa.certify(history, snapshot)：history 是按首次提交顺序排列的成功请求，不含重放；snapshot 是 inspect 返回对象。返回可 JSON 往返的等价证书或带端口、行、截点、期望和实际权重的反例；元数据不一致可返回明确的元数据反例。证据结构由交付者定义并在报告说明，证书须能由独立检查入口 qa.check_certificate(history,snapshot,result) 重算核对；不能只读取 equivalent=true。
证书针对压缩前缀权重的上述全截点不变量，并说明它如何推出关联账的等价性。小域直接求前缀权重与显式多重集关联的穷举须互核证书，同时调用真实 read；大域验证不得按整数展开、枚举各轴坐标的笛卡尔网格、展开所有关联对或逐点调用 read。证书不能依赖坐标大小，也不能通过增加样本数冒充全域判定。
压力数据须允许维数、反链宽度、同键偏斜与正负抵消独立变化，测量生产与判定器各自的耗时及 Python 峰值分配，说明测量口径与适用边界，不设置新的生产性能达标线。
随机失败须保留种子、缩减前后输入、期望/实际及重放方式，缩减保持同一语义偏差。变异在隔离副本或可撤销注入中执行，报告具体变异、正常对照、观测差异与触发输入；导入失败、语法错误或进程崩溃不能算语义变异被检出。并发与故障证据记录调用区间和原始响应，判定实际历史，而非仅比较最终汇总。无法完成的验证和未解决偏差须如实列出。
