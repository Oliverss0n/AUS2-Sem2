import java.io.*;
import java.util.ArrayList;

public class Block<T extends IRecord<T>> {

    private ArrayList<T> list;      // vždy veľkosť blockFactor
    private int validCount;         // počet platných záznamov
    private int blockFactor;
    private T prototype;            // len kvôli getSize()

    public Block(int blockFactor, T prototype, ArrayList<T> emptyList) {
        this.blockFactor = blockFactor;
        this.prototype = prototype;
        this.list = emptyList;     // prázdne T (vytvorené DALEKO MIMO)
        this.validCount = 0;
    }

    public int getValidCount() {
        return validCount;
    }

    public void setValidCount(int validCount) {
        this.validCount = validCount;
    }

    public ArrayList<T> getList() {
        return list;
    }

    public int getBlockSize() {
        return blockFactor * prototype.getSize() + 4; // 4B = validCount
    }

    // ===================================================
    // SERIALIZÁCIA BLOKU DO BAJTOV
    // ===================================================
    public ArrayList<Byte> getBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            // 1) Zapíš všetky T (aj neplatné)
            for (int i = 0; i < blockFactor; i++) {
                ArrayList<Byte> rec = list.get(i).getBytes();
                for (byte b : rec) dos.writeByte(b);
            }

            // 2) Zapíš validCount — jednoducho!
            dos.writeInt(validCount);
            dos.flush();

        } catch (IOException e) {
            throw new RuntimeException("Error serializing block", e);
        }

        // Konverzia na ArrayList<Byte>
        byte[] raw = bos.toByteArray();
        ArrayList<Byte> out = new ArrayList<>(raw.length);
        for (byte b : raw) out.add(b);
        return out;
    }

    // ===================================================
    // DESERIALIZÁCIA BLOKU — BEZ POSUNOV !!
    // ===================================================
    public void fromBytes(ArrayList<Byte> bytes) {
        byte[] raw = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) raw[i] = bytes.get(i);

        ByteArrayInputStream bis = new ByteArrayInputStream(raw);
        DataInputStream dis = new DataInputStream(bis);

        try {
            int recSize = prototype.getSize();

            // 1) načítaj všetky T
            for (int i = 0; i < blockFactor; i++) {

                byte[] slice = new byte[recSize];
                dis.readFully(slice); // načítaj presne recSize bajtov

                ArrayList<Byte> arr = new ArrayList<>(recSize);
                for (byte b : slice) arr.add(b);

                list.get(i).fromBytes(arr);
            }

            // 2) načítaj validCount
            this.validCount = dis.readInt();

        } catch (IOException e) {
            throw new RuntimeException("Error reading block", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Block(valid=" + validCount + "):\n");
        for (int i = 0; i < validCount; i++) {
            sb.append("   ").append(list.get(i)).append("\n");
        }
        return sb.toString();
    }
}
