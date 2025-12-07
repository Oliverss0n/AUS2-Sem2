package Model;

import DataStructures.IRecord;
import java.io.*;
import java.util.ArrayList;

public class PCRTest implements IRecord<PCRTest> {

    private static final int PATIENT_ID_LEN = 10;
    private static final int NOTE_LEN = 11;

    // Atribúty
    private int testCode;           // Unikátny kód PCR testu
    private String patientId;       // ID pacienta

    // ✅ NOVÉ: Dátum a čas rozdelený
    private int year;
    private int month;
    private int day;
    private int hour;
    private int minute;

    private boolean result;         // Výsledok testu
    private double value;           // Hodnota testu
    private String note;            // Poznámka

    public PCRTest() {
        this.testCode = 0;
        this.patientId = "";
        this.year = 0;
        this.month = 0;
        this.day = 0;
        this.hour = 0;
        this.minute = 0;
        this.result = false;
        this.value = 0.0;
        this.note = "";
    }

    /*
    public PCRTest(int testCode, String patientId, long timestamp,
                   boolean result, double value, String note) {
        this.testCode = testCode;
        this.patientId = patientId;

        // ✅ Konverzia timestamp na polia (pre spätkovú kompatibilitu s generátorom)
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        this.year = cal.get(java.util.Calendar.YEAR);
        this.month = cal.get(java.util.Calendar.MONTH) + 1;
        this.day = cal.get(java.util.Calendar.DAY_OF_MONTH);
        this.hour = cal.get(java.util.Calendar.HOUR_OF_DAY);
        this.minute = cal.get(java.util.Calendar.MINUTE);

        this.result = result;
        this.value = value;
        this.note = note;
    }*/


    // ✅ NOVÝ konštruktor s rozdelenými poliami
    public PCRTest(int testCode, String patientId, int year, int month, int day,
                   int hour, int minute, boolean result, double value, String note) {
        this.testCode = testCode;
        this.patientId = patientId;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.result = result;
        this.value = value;
        this.note = note;
    }

    @Override
    public int getSize() {
        // testCode(4) + patientId(10+1) + datum(20) + result(1) + value(8) + note(11+1)
        // datum: year(4) + month(4) + day(4) + hour(4) + minute(4) = 20
        return 4 + (PATIENT_ID_LEN + 1) + 20 + 1 + 8 + (NOTE_LEN + 1);
    }

    @Override
    public ArrayList<Byte> getBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(this.testCode);

            // PATIENT ID
            byte[] idBytes = this.patientId.getBytes();
            dos.write(pad(idBytes, PATIENT_ID_LEN));
            dos.writeByte(Math.min(idBytes.length, PATIENT_ID_LEN));

            // ✅ DATUM A CAS
            dos.writeInt(this.year);
            dos.writeInt(this.month);
            dos.writeInt(this.day);
            dos.writeInt(this.hour);
            dos.writeInt(this.minute);

            dos.writeBoolean(this.result);
            dos.writeDouble(this.value);

            // NOTE
            byte[] noteBytes = this.note.getBytes();
            dos.write(pad(noteBytes, NOTE_LEN));
            dos.writeByte(Math.min(noteBytes.length, NOTE_LEN));

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

    @Override
    public void fromBytes(ArrayList<Byte> a) {
        try {
            byte[] arr = new byte[a.size()];
            for (int i = 0; i < a.size(); i++)
                arr[i] = a.get(i);

            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(arr));

            this.testCode = dis.readInt();

            // PATIENT ID
            byte[] tmp = new byte[PATIENT_ID_LEN];
            dis.readFully(tmp);
            int idReal = dis.readUnsignedByte();
            this.patientId = new String(tmp, 0, idReal);

            // ✅ DATUM A CAS
            this.year = dis.readInt();
            this.month = dis.readInt();
            this.day = dis.readInt();
            this.hour = dis.readInt();
            this.minute = dis.readInt();

            this.result = dis.readBoolean();
            this.value = dis.readDouble();

            // NOTE
            tmp = new byte[NOTE_LEN];
            dis.readFully(tmp);
            int noteReal = dis.readUnsignedByte();
            this.note = new String(tmp, 0, noteReal);

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

    @Override
    public boolean isEqual(PCRTest test) {
        return test != null && this.testCode == test.testCode;
    }

    @Override
    public int getHashCode() {
        return Math.abs(testCode);
    }

    @Override
    public PCRTest createEmpty() {
        return new PCRTest(0, "", 0, 0, 0, 0, 0, false, 0.0, "");
    }

    // ✅ Gettery/Settery
    public int getTestCode() { return testCode; }
    public void setTestCode(int testCode) { this.testCode = testCode; }

    public String getPatientId() { return patientId; }
    public void setPatientId(String patientId) { this.patientId = patientId; }

    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }

    public int getDay() { return day; }
    public void setDay(int day) { this.day = day; }

    public int getHour() { return hour; }
    public void setHour(int hour) { this.hour = hour; }

    public int getMinute() { return minute; }
    public void setMinute(int minute) { this.minute = minute; }

    public boolean isResult() { return result; }
    public void setResult(boolean result) { this.result = result; }

    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    public String getNote() { return note; }
    public void setNote(String note) { this.note = note; }

    @Override
    public String toString() {
        return "PCR Test #" + testCode +
                " (Patient: " + patientId +
                ", Date: " + year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) +
                " " + String.format("%02d", hour) + ":" + String.format("%02d", minute) +
                ", Result: " + (result ? "Positive" : "Negative") +
                ", Value: " + value + ")";
    }
}