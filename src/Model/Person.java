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
        return NAME_LEN + SURNAME_LEN + ID_LEN + 12;
    }

    //zapisanie
    @Override
    public ArrayList<Byte> getBytes() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // NAME
            dos.write(pad(this.name.getBytes(), NAME_LEN));

            // SURNAME
            dos.write(pad(this.surname.getBytes(), SURNAME_LEN));

            // ID
            dos.write(pad(this.id.getBytes(), ID_LEN));

            // DATE
            dos.writeInt(this.year);
            dos.writeInt(this.month);
            dos.writeInt(this.day);

            byte[] arr = baos.toByteArray();
            ArrayList<Byte> out = new ArrayList<>(arr.length);
            for (byte b : arr) {
                out.add(b);
            }
            return out;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //nacitanie
    @Override
    public void fromBytes(ArrayList<Byte> a) {

        try {
            byte[] arr = new byte[a.size()];
            for (int i = 0; i < a.size(); i++) {
                arr[i] = a.get(i);
            }

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));

            byte[] tmp;

            // NAME
            tmp = new byte[NAME_LEN];
            dis.readFully(tmp);
            name = new String(tmp).trim();

            // SURNAME
            tmp = new byte[SURNAME_LEN];
            dis.readFully(tmp);
            this.surname = new String(tmp).trim();

            // ID
            tmp = new byte[ID_LEN];
            dis.readFully(tmp);
            this.id = new String(tmp).trim();

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
            if (i < before.length) {
                out[i] = before[i];
            } else {
                out[i] = 0;
            }
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
        return this.name + " " + this.surname + " " + this.id + " (" + this.year + "-" + this.month + "-" + this.day + ")";
    }
}
