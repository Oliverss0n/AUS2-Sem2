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
        return NAME_LEN + 4 + SURNAME_LEN + 4 + ID_LEN + 4 + 12;
    }

    // ======================================================
    // BYTE ARRAY -> univerzálna metóda, najjednoduchšie
    // ======================================================
    @Override
    public ArrayList<Byte> getBytes() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // NAME
            byte[] nb = name.getBytes();
            dos.write(pad(nb, NAME_LEN));
            dos.writeInt(nb.length);

            // SURNAME
            byte[] sb = surname.getBytes();
            dos.write(pad(sb, SURNAME_LEN));
            dos.writeInt(sb.length);

            // ID
            byte[] ib = id.getBytes();
            dos.write(pad(ib, ID_LEN));
            dos.writeInt(ib.length);

            // DATE
            dos.writeInt(year);
            dos.writeInt(month);
            dos.writeInt(day);

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
            for (int i = 0; i < a.size(); i++) {
                arr[i] = a.get(i);
            }

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));

            byte[] tmp;

            // NAME
            tmp = new byte[NAME_LEN];
            dis.readFully(tmp);
            name = new String(tmp, 0, dis.readInt());

            // SURNAME
            tmp = new byte[SURNAME_LEN];
            dis.readFully(tmp);
            surname = new String(tmp, 0, dis.readInt());

            // ID
            tmp = new byte[ID_LEN];
            dis.readFully(tmp);
            id = new String(tmp, 0, dis.readInt());

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

    @Override
    public String toString() {
        return name + " " + surname + " " + id + " (" + year + "-" + month + "-" + day + ")";
    }
}
