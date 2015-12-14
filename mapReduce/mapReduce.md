###mapReduce
**描述：**常用的mapReduce方法 

####GetStatusByKey
**描述：**计算出某天发表的包含指定关键字的微博数
**输入：**关键字列表、微博集合
**输出：**关键字  微博创建时间(yyyy-MM-dd)  微博数(该天中创建的包含该关键字的微博数量)

####GetStatusByTime
**描述：**筛选出特定时间段内发表的微博
**输入：**startTime和endTime、微博集合
**输出：**微博mid  微博内容

####GetStatusByUid
**描述：**筛选出特定用户发表的微博
**输入：**UID、微博集合
**输出：**微博mid  微博内容

####WordCount
**描述：**讲解mapReduce程序的写法

