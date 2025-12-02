package DataStructures;

import java.io.*;
import java.util.ArrayList;

public class Block<T extends IRecord<T>> {

    private ArrayList<T> list;
    private int validCount;
    private int blockFactor;
    private T prototype;

    private long next;


    public Block(int blockFactor, T prototype, ArrayList<T> emptyList) {
        this.blockFactor = blockFactor;
        this.prototype = prototype;
        this.list = emptyList;
        this.validCount = 0;
        this.next = -1;
    }

    public int getValidCount() {
        return this.validCount;
    }

    public void setValidCount(int validCount) {
        this.validCount = validCount;
    }

    public ArrayList<T> getList() {
        return list;
    }

    /* bez next
    public ArrayList<Byte> getBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            for (int i = 0; i < this.blockFactor; i++) {

                ArrayList<Byte> recordBytes = list.get(i).getBytes();

                for (byte byteVal : recordBytes) {
                    dos.writeByte(byteVal);
                }
            }

            dos.writeInt(this.validCount);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] blockBytes = bos.toByteArray();

        ArrayList<Byte> result = new ArrayList<>(blockBytes.length);
        for (byte byteVal : blockBytes) {
            result.add(byteVal);
        }

        return result;
    }*/

    public ArrayList<Byte> getBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            for (int i = 0; i < this.blockFactor; i++) {

                ArrayList<Byte> recordBytes = list.get(i).getBytes();

                for (byte byteVal : recordBytes) {
                    dos.writeByte(byteVal);
                }
            }

            dos.writeInt(this.validCount);

            dos.writeLong(this.next);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] blockBytes = bos.toByteArray();

        ArrayList<Byte> result = new ArrayList<>(blockBytes.length);
        for (byte byteVal : blockBytes) {
            result.add(byteVal);
        }

        return result;
    }



/*
    public void fromBytes(ArrayList<Byte> bytes) {
        byte[] raw = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) raw[i] = bytes.get(i);

        ByteArrayInputStream bis = new ByteArrayInputStream(raw);
        DataInputStream dis = new DataInputStream(bis);

        try {
            int recSize = prototype.getSize();

            for (int i = 0; i < blockFactor; i++) {

                byte[] slice = new byte[recSize];
                dis.readFully(slice);

                ArrayList<Byte> arr = new ArrayList<>(recSize);
                for (byte b : slice) arr.add(b);

                list.get(i).fromBytes(arr);
            }

            this.validCount = dis.readInt();

        } catch (IOException e) {
            throw new RuntimeException("chyba v fromBytes", e);
        }
    }*/

    public void fromBytes(ArrayList<Byte> bytes) {
        byte[] raw = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) raw[i] = bytes.get(i);

        ByteArrayInputStream bis = new ByteArrayInputStream(raw);
        DataInputStream dis = new DataInputStream(bis);

        try {
            int recSize = prototype.getSize();

            for (int i = 0; i < blockFactor; i++) {

                byte[] slice = new byte[recSize];
                dis.readFully(slice);

                ArrayList<Byte> arr = new ArrayList<>(recSize);
                for (byte b : slice) arr.add(b);

                list.get(i).fromBytes(arr);
            }

            this.validCount = dis.readInt();

            this.next = dis.readLong();

        } catch (IOException e) {
            throw new RuntimeException("chyba v fromBytes", e);
        }
    }


    public long getNext() { return next; }
    public void setNext(long n) { next = n; }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("validCount=").append(validCount).append("\n");

        for (int i = 0; i < validCount; i++) {
            sb.append("  ").append(i).append(": ").append(list.get(i)).append("\n");
        }
        return sb.toString();
    }

}
