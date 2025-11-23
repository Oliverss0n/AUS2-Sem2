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
        name = "";
        surname = "";
        id = "";
        year = 2000;
        month = 1;
        day = 1;
    }

    public Person(String n, String s, String i, int y, int m, int d) {
        name = n;
        surname = s;
        id = i;
        year = y;
        month = m;
        day = d;
    }

    @Override
    public boolean isEqual(Person p) {
        return p != null && id.equals(p.id);
    }

    @Override
    public int getSize() {
        // LEN fixné dĺžky + dátum (3×int)
        return NAME_LEN + SURNAME_LEN + ID_LEN + 12;
    }


    // ======================================================
    // BYTE ARRAY -> univerzálna metóda, najjednoduchšie
    // ======================================================
    @Override
    public ArrayList<Byte> getBytes() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // NAME (fixed length)
            dos.write(pad(name.getBytes(), NAME_LEN));

            // SURNAME
            dos.write(pad(surname.getBytes(), SURNAME_LEN));

            // ID
            dos.write(pad(id.getBytes(), ID_LEN));

            // DATE
            dos.writeInt(year);
            dos.writeInt(month);
            dos.writeInt(day);

            // return ArrayList<Byte>
            byte[] arr = baos.toByteArray();
            ArrayList<Byte> out = new ArrayList<>(arr.length);
            for (byte b : arr) out.add(b);
            return out;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void fromBytes(ArrayList<Byte> a) {

        try {
            byte[] arr = new byte[a.size()];
            for (int i = 0; i < a.size(); i++) arr[i] = a.get(i);

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));

            byte[] tmp;

            // NAME
            tmp = new byte[NAME_LEN];
            dis.readFully(tmp);
            name = new String(tmp).trim();

            // SURNAME
            tmp = new byte[SURNAME_LEN];
            dis.readFully(tmp);
            surname = new String(tmp).trim();

            // ID
            tmp = new byte[ID_LEN];
            dis.readFully(tmp);
            id = new String(tmp).trim();

            // DATE
            year = dis.readInt();
            month = dis.readInt();
            day = dis.readInt();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // doplnenie nulami
    /*private byte[] pad(byte[] src, int len) {
        byte[] out = new byte[len];
        System.arraycopy(src, 0, out, 0, Math.min(src.length, len));
        return out;
    }*/
    private byte[] pad(byte[] src, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            if (i < src.length) {
                out[i] = src[i];
            } else {
                out[i] = 0;
            }
        }
        return out;
    }


    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getId() {
        return id;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public void fromId(String newId) {
        this.id = newId;
    }


    @Override
    public String toString() {
        return name + " " + surname + " " + id + " (" + year + "-" + month + "-" + day + ")";
    }
}
