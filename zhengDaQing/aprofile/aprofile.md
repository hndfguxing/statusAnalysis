##aprofile
爬取用户的个人信息，uid.txt中为42个用户的uid，profile.txt中为42个用户的个人信息。
  
####BshowUserByDomainOrUID
**描述：**根据domain或者UID获取user信息  
**输入：**AuserDomainOrUID  
**输出：**Buser  

####CgetProfileFromUser
**描述：**根据user获取profile	 
**输入：**Buser  
**输出：**Cprofile  

####DgetScreenNameByProfile
**描述：**根据profile获取screenName	
**输入：**Cprofile  
**输出：**DscreenName  

####EgetUIDByProfile
**描述：**根据profile获取UID  
**输入：**Cprofile  
**输出：**EuID  

####FcombineScreenNameAndUID
**描述：**根据screenName和UID输出screenNameAndUID	 
**输入：**DscreenName、EuID  
**输出：**FscreenNameAndUID  

####GgetVFollowerCountByUID
**描述：**根据UID获取粉丝中大V数量  
**输入：**EuID  
**输出：**GvFollowerCount  

####HcombineProfileAndVFollowerCount
**描述：**根据profile和vFollowerCount获取新的profile	
**输入：**Cprofile、GvFollowerCount  
**输出：**HprofileNew	

