package DataStructures;

import java.util.ArrayList;

public interface IRecord<T> {
    boolean isEqual(T data);
    int getSize();

    ArrayList<Byte> getBytes();

    void fromBytes(ArrayList<Byte> byteArray);

    int getHashCode();

}
