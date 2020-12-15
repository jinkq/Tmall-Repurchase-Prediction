package FindHottestItemsAndPopularMerchants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserLog implements Writable{
    private String sellerID; //卖家ID
    private Boolean userAge; //买家是否符合年龄小于30岁的要求

    public UserLog(){
        this.sellerID = "";
        this.userAge = false;
    }

    public UserLog(String sellerID, Boolean userAge) {
        this.sellerID = sellerID;
        this.userAge = userAge;
    }

    @Override
    public void write(DataOutput out) throws IOException {//序列化
        out.writeUTF(sellerID);
        out.writeBoolean(userAge);
    }

    @Override
    public void readFields(DataInput in) throws IOException {//反序列化
        this.sellerID = in.readUTF();
        this.userAge = in.readBoolean();
    }

    public String getSellerID() {
        return sellerID;
    }

    public void setSellerID(String sellerID) {
        this.sellerID = sellerID;
    }

    public Boolean getUserAge() {
        return userAge;
    }
    
    public void setUserAge(Boolean userAge) {
        this.userAge = userAge;
    }
}