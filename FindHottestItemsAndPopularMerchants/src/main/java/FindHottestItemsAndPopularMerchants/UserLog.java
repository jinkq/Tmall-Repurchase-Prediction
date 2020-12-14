import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserLog implements Writable{
    private String userID, sellerID;

    public UserLog()//这个默认构造函数需要，否则会报错
    {

    }

    public UserLog(String userID,  String sellerID) {
        this.userID=userID;
        this.sellerID=sellerID;
    }
    @Override
    public void write(DataOutput out) throws IOException {//序列化
        // TODO Auto-generated method stub
        out.writeUTF(userID);
        out.writeUTF(sellerID);
    }

    @Override
    public void readFields(DataInput in) throws IOException {//反序列化
        // TODO Auto-generated method stub
        this.userID=in.readUTF();
        this.sellerID=in.readUTF();
    }

    // @Override
    // public String toString() {
    //     // TODO Auto-generated method stub
    //     return this.userID+" "+this.ageRange+" "+this.sellerID+" "+this.actionType;
    // }

    public String getUserID() {
        return userID;
    }
    public void setUserID(String userID) {
        this.userID = userID;
    }
    public String getSellerID() {
        return sellerID;
    }
    public void setSellerID(String sellerID) {
        this.sellerID = sellerID;
    }
}