package com.focusmedia.newgenerate;

import com.focusmedia.util.DevDateUtil;

import java.io.Serializable;

public class RealPlayLog implements Serializable{

    public String devid;
    public String start_time;
    public long start_time_long;
    public String end_time;
    public long end_time_long;
    public int sch_version;
    public int pls_index;
    public int play_index;
    public boolean isseed;
    public long p_length;//代表已播时长
    public long ad_length;//代表本身广告长度,转化为毫秒的长度
    public long length_left;//代表未播放的广告长度
    public String pln_info;//代表广告订单号
    public String seqkey;
    public int uploadcount;

    public RealPlayLog(){}


    public RealPlayLog(String devid, String seqkey, long start_time_long, long ad_length, String pln_id){
        this(devid,seqkey,start_time_long,ad_length,ad_length,pln_id);
    }

    public RealPlayLog(String devid, String seqkey, long end_time_long, long ad_length, String pln_id,String flag){
        this(devid,"",0,"",end_time_long,0,0,0,false,ad_length,ad_length,0,pln_id,seqkey,0);
        this.end_time=DevDateUtil.longTimeToStr(end_time_long);
        this.start_time_long=this.end_time_long-this.ad_length;
        this.start_time=DevDateUtil.longTimeToStr(start_time_long);
        String[] cols = seqkey.split(",");
        this.sch_version=Integer.parseInt(cols[0]);
        this.uploadcount=Integer.parseInt(cols[1]);
        this.pls_index=Integer.parseInt(cols[2]);
        this.play_index=Integer.parseInt(cols[3]);
        this.isseed=true;
    }

    public RealPlayLog(String devid,String seqKey,long start_time_long,long p_length,long ad_length,String pln_id){
        this(devid,"",start_time_long,"",0,0,0,0,false,p_length,ad_length,0,pln_id,seqKey,0);
        this.start_time= DevDateUtil.longTimeToStr(start_time_long);
        this.end_time_long=this.start_time_long+p_length;
        this.end_time= DevDateUtil.longTimeToStr(end_time_long);

        String[] cols=seqKey.split(",");
        this.sch_version=Integer.parseInt(cols[0]);
        this.uploadcount=Integer.parseInt(cols[1]);
        this.pls_index=Integer.parseInt(cols[2]);
        this.play_index=Integer.parseInt(cols[3]);

        this.length_left=this.ad_length-this.p_length;
        if(this.length_left<=0){
            this.length_left=0;
            this.isseed=true;
        }

    }
    //extend 扩展上一条的结束时间
    public void updateEndTime(long tmp_sub_time) {
        this.end_time_long +=tmp_sub_time;
        this.end_time= DevDateUtil.longTimeToStr(end_time_long);
        this.p_length +=tmp_sub_time;   //代表已播放的长度 毫秒
        this.length_left -=tmp_sub_time;    //代表还未播放的长度 毫秒
        if (this.length_left <= 0) {
            this.length_left = 0;
            this.isseed = true;
        }
    }

    //将seed的开始时间付给上一条的结束时间
    public void changePreEndTime(long meend_time) {
        this.end_time_long=meend_time;
        this.end_time = DevDateUtil.longTimeToStr(end_time_long);
        p_length = end_time_long - start_time_long;
        length_left = ad_length - p_length;
        if (length_left <= 0) {
            length_left = 0;
            isseed = true;
        } else {
            isseed = false;
        }
    }

    //在结束时间正常的情况下，直接更新开始时间
    public void changePreStartTime() {
        this.start_time_long=this.end_time_long-this.ad_length;
        this.start_time= DevDateUtil.longTimeToStr(this.start_time_long);
        this.p_length=this.end_time_long-this.start_time_long;
        this.length_left=this.ad_length-this.p_length;
        if(this.length_left<=0){
            this.length_left=0;
            this.isseed=true;
        }else{
            this.isseed=false;
        }

    }

    //用来用上一条seed的结束时间来替换当前条的开始时间
    public void replaceStartTime(long mestart_time) {
        this.start_time_long =mestart_time;
        this.start_time = DevDateUtil.longTimeToStr(start_time_long);
        this.p_length = this.end_time_long - this.start_time_long;
        this.length_left = this.ad_length - this.p_length;
        if (this.length_left <= 0) {
            this.length_left = 0;
            this.isseed = true;
        } else {
            this.isseed = false;
        }
    }

    public void replaceEndTime() {
        this.end_time_long=this.start_time_long+this.ad_length;
        this.end_time= DevDateUtil.longTimeToStr(end_time_long);
        this.p_length=this.end_time_long-this.start_time_long;
        this.length_left=this.ad_length-this.p_length;
        if(this.length_left<=0){
            this.isseed=true;
        }else{
            this.isseed=false;
        }
    }


    public void completeStartAndEnd(long next_start_time_long) {
        this.end_time_long=next_start_time_long;
        this.end_time= DevDateUtil.longTimeToStr(this.end_time_long);
        this.start_time_long=this.end_time_long-this.ad_length;
        this.start_time= DevDateUtil.longTimeToStr(this.start_time_long);
        this.p_length=this.end_time_long-this.start_time_long;
        this.length_left=this.ad_length-this.p_length;
        if(this.length_left<=0){
            this.length_left=0;
            this.isseed=true;
        }else{
            this.isseed=false;
        }
    }




    //起始播放时间往前移
    public void extendstart(Long addtime){
        this.start_time_long -= addtime;
        this.start_time = DevDateUtil.longTimeToStr(this.start_time_long);
        this.p_length += addtime;
        this.length_left -= addtime;
        if (this.length_left <= 0) {
            this.length_left = 0;
            this.isseed = true;
        }
    }

    //补endtime
    public void  extendend( Long addtime){
        this.end_time_long += addtime;
        this.end_time = DevDateUtil.longTimeToStr(this.end_time_long);
        this.p_length += addtime;
        this.length_left -= addtime;
        if (this.length_left <= 0) {
            this.length_left = 0;
            this.isseed = true;
        }
    }




    public RealPlayLog(String devid, String start_time, long start_time_long, String end_time, long end_time_long, int sch_version, int pls_index, int play_index, boolean isseed, long p_length, long ad_length, long length_left, String pln_info, String seqkey,int uploadcount) {
        this.devid = devid;
        this.start_time = start_time;
        this.start_time_long = start_time_long;
        this.end_time = end_time;
        this.end_time_long = end_time_long;
        this.sch_version = sch_version;
        this.pls_index = pls_index;
        this.play_index = play_index;
        this.isseed = isseed;
        this.p_length = p_length;
        this.ad_length = ad_length;
        this.length_left = length_left;
        this.pln_info = pln_info;
        this.seqkey = seqkey;
        this.uploadcount=uploadcount;
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public long getStart_time_long() {
        return start_time_long;
    }

    public void setStart_time_long(long start_time_long) {
        this.start_time_long = start_time_long;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public long getEnd_time_long() {
        return end_time_long;
    }

    public void setEnd_time_long(long end_time_long) {
        this.end_time_long = end_time_long;
    }

    public int getSch_version() {
        return sch_version;
    }

    public void setSch_version(int sch_version) {
        this.sch_version = sch_version;
    }

    public int getPls_index() {
        return pls_index;
    }

    public void setPls_index(int pls_index) {
        this.pls_index = pls_index;
    }

    public int getPlay_index() {
        return play_index;
    }

    public void setPlay_index(int play_index) {
        this.play_index = play_index;
    }

    public boolean isIsseed() {
        return isseed;
    }

    public void setIsseed(boolean isseed) {
        this.isseed = isseed;
    }

    public long getP_length() {
        return p_length;
    }

    public void setP_length(long p_length) {
        this.p_length = p_length;
    }

    public long getAd_length() {
        return ad_length;
    }

    public void setAd_length(long ad_length) {
        this.ad_length = ad_length;
    }

    public long getLength_left() {
        return length_left;
    }

    public void setLength_left(long length_left) {
        this.length_left = length_left;
    }

    public String getPln_info() {
        return pln_info;
    }

    public void setPln_info(String pln_info) {
        this.pln_info = pln_info;
    }

    public String getSeqkey() {
        return seqkey;
    }

    public void setSeqkey(String seqkey) {
        this.seqkey = seqkey;
    }

    public int getUploadcount() {
        return uploadcount;
    }

    public void setUploadcount(int uploadcount) {
        this.uploadcount = uploadcount;
    }

    @Override
    public String toString() {
        return "RealPlayLog{" +
                "devid='" + devid + '\'' +
                ", start_time='" + start_time + '\'' +
                ", start_time_long=" + start_time_long +
                ", end_time='" + end_time + '\'' +
                ", end_time_long=" + end_time_long +
                ", sch_version=" + sch_version +
                ", pls_index=" + pls_index +
                ", play_index=" + play_index +
                ", isseed=" + isseed +
                ", p_length=" + p_length +
                ", ad_length=" + ad_length +
                ", length_left=" + length_left +
                ", pln_info='" + pln_info + '\'' +
                ", seqkey='" + seqkey + '\'' +
                ", uploadcount=" + uploadcount +
                '}';
    }
}
