package Model;

import DataStructures.IRecord;

import java.io.*;
import java.util.ArrayList;

public class Person implements IRecord<Person> {

    private static final int NAME_LEN = 15;
    private static final int SURNAME_LEN = 14;
    private static final int ID_LEN = 10;

    private String name;
    private String surname;
    private String id;
    private int year, month, day;

    public Person() {
        this.name = "";
        this.surname = "";
        this.id = "";
        this.year = 0;
        this.month = 0;
        this.day = 0;
    }

    public Person(String n, String s, String i, int y, int m, int d) {
        this.name = n;
        this.surname = s;
        this.id = i;
        this.year = y;
        this.month = m;
        this.day = d;
    }

    @Override
    public boolean isEqual(Person p) {
        return p != null && id.equals(p.id);
    }

    @Override
    public int getSize() {
        // 15 + 1 + 14 + 1 + 10 + 1 + 12 = 54
        return NAME_LEN + 1 + SURNAME_LEN + 1 + ID_LEN + 1 + 12;
    }

    // zapisanie
    @Override
    public ArrayList<Byte> getBytes() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // NAME
            byte[] nameBytes = this.name.getBytes();
            dos.write(pad(nameBytes, NAME_LEN));
            dos.writeByte(Math.min(nameBytes.length, NAME_LEN));

            // SURNAME
            byte[] surBytes = this.surname.getBytes();
            dos.write(pad(surBytes, SURNAME_LEN));
            dos.writeByte(Math.min(surBytes.length, SURNAME_LEN));

            // ID
            byte[] idBytes = this.id.getBytes();
            dos.write(pad(idBytes, ID_LEN));
            dos.writeByte(Math.min(idBytes.length, ID_LEN));

            // DATE
            dos.writeInt(this.year);
            dos.writeInt(this.month);
            dos.writeInt(this.day);

            byte[] arr = baos.toByteArray();
            ArrayList<Byte> out = new ArrayList<>(arr.length);
            for (byte b : arr) out.add(b);

            return out;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // nacitanie
    @Override
    public void fromBytes(ArrayList<Byte> a) {

        try {
            byte[] arr = new byte[a.size()];
            for (int i = 0; i < a.size(); i++)
                arr[i] = a.get(i);

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));

            byte[] tmp;

            // NAME
            tmp = new byte[NAME_LEN];
            dis.readFully(tmp);
            int nameReal = dis.readUnsignedByte();
            this.name = new String(tmp, 0, nameReal);

            // SURNAME
            tmp = new byte[SURNAME_LEN];
            dis.readFully(tmp);
            int surReal = dis.readUnsignedByte();
            this.surname = new String(tmp, 0, surReal);

            // ID
            tmp = new byte[ID_LEN];
            dis.readFully(tmp);
            int idReal = dis.readUnsignedByte();
            this.id = new String(tmp, 0, idReal);

            // DATE
            this.year = dis.readInt();
            this.month = dis.readInt();
            this.day = dis.readInt();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] pad(byte[] before, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (i < before.length) ? before[i] : 0;
        }
        return out;
    }

    public void fromId(String newId) {
        this.id = newId;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return this.name + " " + this.surname + " " + this.id +
                " (" + this.year + "-" + this.month + "-" + this.day + ")";
    }

    @Override
    public int getHashCode() {
        return id.hashCode();
    }

}
