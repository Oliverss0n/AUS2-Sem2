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

    public void setValidCount(int vc) {
        this.validCount = vc;
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
        ArrayList<Byte> out = new ArrayList<>(getBlockSize());

        // Zapíš všetky T-čka (aj neplatné)
        for (int i = 0; i < blockFactor; i++) {
            out.addAll(list.get(i).getBytes());
        }

        // Zapíš validCount
        out.add((byte)(validCount >> 24));
        out.add((byte)(validCount >> 16));
        out.add((byte)(validCount >> 8));
        out.add((byte)(validCount));

        return out;
    }

    // ===================================================
    // DESERIALIZÁCIA BLOKU Z BAJTOV
    // ===================================================
    public void fromBytes(ArrayList<Byte> bytes) {

        int pos = 0;
        int recSize = prototype.getSize();

        for (int i = 0; i < blockFactor; i++) {

            T obj = list.get(i);  // už existujúce T vytvorené mimo blok

            ArrayList<Byte> slice = new ArrayList<>(recSize);
            for (int j = 0; j < recSize; j++) {
                slice.add(bytes.get(pos + j));
            }

            obj.fromBytes(slice);

            pos += recSize;
        }

        // načítaj validCount
        int vc =
                ((bytes.get(pos) & 0xFF) << 24) |
                        ((bytes.get(pos+1) & 0xFF) << 16) |
                        ((bytes.get(pos+2) & 0xFF) << 8) |
                        ((bytes.get(pos+3) & 0xFF));

        this.validCount = vc;
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
