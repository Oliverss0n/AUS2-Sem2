import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
public class Main {
    public static void main(String[] args) {


    }

}
public byte[] ToByteArray() {
    ByteArrayOutputStream hlpByteArrayOutputStream= new ByteArrayOutputStream();
    DataOutputStream hlpOutStream = new DataOutputStream(hlpByteArrayOutputStream);
    try{
        hlpOutStream.writeInt(this.NejakyAtributInt);

        return hlpByteArrayOutputStream.toByteArray();

    }catch (IOException e){
        throw new IllegalStateException("Error during conversion to byte array.");
    }

    public void FromByteArray(byte[] paArray) {
        ByteArrayInputStream hlpByteArrayInputStream = new ByteArrayInputStream(paArray);
        DataInputStream hlpInStream = new DataInputStream(hlpByteArrayInputStream);
        try {
            this.NejakyAtributInt = hlpInStream.readInt();

        } catch (IOException e) {
            throw new IllegalStateException("Error during conversion from byte array.");
        }
    }