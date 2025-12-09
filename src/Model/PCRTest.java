package Model;

import DataStructures.IRecord;
import java.io.*;
import java.util.ArrayList;

public class PCRTest implements IRecord<PCRTest> {

    private static final int PATIENT_ID_LEN = 10;
    private static final int NOTE_LEN = 11;

    private int testCode;
    private String patientId;

    private long timestamp;

    private boolean result;
    private double value;
    private String note;

    public PCRTest() {
        this.testCode = 0;
        this.patientId = "";
        this.timestamp = 0L;
        this.result = false;
        this.value = 0.0;
        this.note = "";
    }

    public PCRTest(int testCode, String patientId, long timestamp,
                   boolean result, double value, String note) {
        this.testCode = testCode;
        this.patientId = patientId;
        this.timestamp = timestamp;
        this.result = result;
        this.value = value;
        this.note = note;
    }

    public static long makeTimestamp(int year, int month, int day, int hour, int minute) {
        //validacia pre rozsahy
        if (year < 1000 || year > 9999) throw new IllegalArgumentException("Invalid year: " + year);
        if (month < 1 || month > 12) throw new IllegalArgumentException("Invalid month: " + month);
        if (day < 1 || day > 31) throw new IllegalArgumentException("Invalid day: " + day);
        if (hour < 0 || hour > 23) throw new IllegalArgumentException("Invalid hour: " + hour);
        if (minute < 0 || minute > 59) throw new IllegalArgumentException("Invalid minute: " + minute);

        return (long) year * 100000000L +
                (long) month * 1000000L +
                (long) day * 10000L +
                (long) hour * 100L +
                (long) minute;
    }

    @Override
    public int getSize() {
        // testCode(4) + patientId(10+1) + timestamp(8) + result(1) + value(8) + note(11+1)
        return 4 + (PATIENT_ID_LEN + 1) + 8 + 1 + 8 + (NOTE_LEN + 1);
    }

    @Override
    public ArrayList<Byte> getBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(this.testCode);

            byte[] idBytes = this.patientId.getBytes();
            dos.write(pad(idBytes, PATIENT_ID_LEN));
            dos.writeByte(Math.min(idBytes.length, PATIENT_ID_LEN));

            dos.writeLong(this.timestamp);

            dos.writeBoolean(this.result);
            dos.writeDouble(this.value);

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

            byte[] tmp = new byte[PATIENT_ID_LEN];
            dis.readFully(tmp);
            int idReal = dis.readUnsignedByte();
            this.patientId = new String(tmp, 0, idReal);

            this.timestamp = dis.readLong();

            this.result = dis.readBoolean();
            this.value = dis.readDouble();

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
        return new PCRTest(0, "", 0L, false, 0.0, "");
    }


    public int getTestCode() { return testCode; }
    public void setTestCode(int testCode) { this.testCode = testCode; }

    public String getPatientId() { return patientId; }
    public void setPatientId(String patientId) { this.patientId = patientId; }

    public int getYear() {
        return (int) (timestamp / 100000000L);
    }

    public int getMonth() {
        return (int) ((timestamp / 1000000L) % 100);
    }

    public int getDay() {
        return (int) ((timestamp / 10000L) % 100);
    }

    public int getHour() {
        return (int) ((timestamp / 100L) % 100);
    }

    public int getMinute() {
        return (int) (timestamp % 100);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

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
                ", Date: " + getYear() + "-" + String.format("%02d", getMonth()) + "-" + String.format("%02d", getDay()) +
                " " + String.format("%02d", getHour()) + ":" + String.format("%02d", getMinute()) +
                ", Result: " + (result ? "Positive" : "Negative") +
                ", Value: " + value + ")";
    }
}