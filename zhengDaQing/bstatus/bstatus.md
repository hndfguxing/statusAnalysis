##bstatus
每个用户微博的统计信息（属性依次是微博id 微博内容 评论数 转发数 点赞数 是否含图片(1含0不含) url）  

####BScreenStatusAndRetweet
**描述：**根据微博数据库获取特定用户发表的微博以及转发特定用户微博的微博	
**输入：**微博集合  
**输出：**输出bstatus/bpart  

####CMIDListAndstatusAndRStatus
**描述：**从part-00000文件中删去UID，以UID命名文件，并从中取出每个用户的MID列表  
**输入：**apart/part-00000  
**输出：**cmIDList/uID、cstatus/uID  

####CMIDListAndstatusAndRStatus
**描述：**从part-00000文件中删去RUID，以UID命名文件	  
**输入：**apart/part-00000  
**输出：**crStatus/uID  

####DGetUserTimeline
**描述：**根据UID爬取最新的2000条微博	
**输入：**aprofile/Cprofile  
**输出：**dstatus/screenNameCount  


