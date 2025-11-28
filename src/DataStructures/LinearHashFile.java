package DataStructures;

import java.io.*;
import java.util.*;

public class LinearHashFile<T extends IRecord<T>> {

    private HeapFile<T> mainFile;
    private HeapFile<T> overflowFile;

    private int M;
    private int u;
    private int S;

    private double d_max;
    private double d_min;

    private int totalRecords;
    private int[] recordCountPerIndex;
    private int[] overflowChainLength;

    private String metadataPath;
    private T prototype;

    public LinearHashFile(String mainPath,
                          int mainBlockSize,
                          String overflowPath,
                          int overflowBlockSize,
                          int M,
                          T prototype) throws Exception {

        this.prototype = prototype;

        this.M = M;
        this.u = 0;
        this.S = 0;

        this.d_max = 0.8;
        this.d_min = 0.64;

        this.totalRecords = 0;

        this.metadataPath = mainPath + ".lh.meta";

        int max = M * (1 << 10);
        this.recordCountPerIndex = new int[max];
        this.overflowChainLength = new int[max];

        this.mainFile = new HeapFile<>(mainPath, mainBlockSize, prototype);
        this.overflowFile = new HeapFile<>(overflowPath, overflowBlockSize, prototype);

        if (mainFile.getFileLength() == 0) {
            createPrimaryBlocks();
        }
    }

    private void createPrimaryBlocks() throws Exception {
        for (int i = 0; i < M; i++) {
            Block<T> empty = mainFile.createEmptyBlock();
            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);
        }
    }

    public void insert(T record) throws Exception {

        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);

        if (block.getValidCount() < mainFile.getBlockFactor()) {

            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            mainFile.writeBlock(offset, block);

            totalRecords++;
            recordCountPerIndex[index]++;

            if (getDensity() > d_max) split();
            return;
        }

        insertIntoOverflow(index, record);
    }

    private void insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();

        if (nextAddr == 0) {

            Block<T> newBlock = overflowFile.createEmptyBlock();

            newBlock.getList().set(0, record);
            newBlock.setValidCount(1);

            long newAddr = overflowFile.writeNewBlock(newBlock);

            primary.setNext(newAddr);
            mainFile.writeBlock(primaryAddr, primary);

            overflowChainLength[index]++;
            totalRecords++;
            return;
        }

        long currentAddr = nextAddr;
        Block<T> current = overflowFile.readBlock(currentAddr);

        while (true) {

            if (current.getValidCount() < overflowFile.getBlockFactor()) {

                current.getList().set(current.getValidCount(), record);
                current.setValidCount(current.getValidCount() + 1);

                overflowFile.writeBlock(currentAddr, current);

                totalRecords++;
                return;
            }

            if (current.getNext() == 0) {

                Block<T> newBlock = overflowFile.createEmptyBlock();

                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);

                long newAddr = overflowFile.writeNewBlock(newBlock);

                current.setNext(newAddr);
                overflowFile.writeBlock(currentAddr, current);

                overflowChainLength[index]++;
                totalRecords++;
                return;
            }

            currentAddr = current.getNext();
            current = overflowFile.readBlock(currentAddr);
        }
    }

    public T find(T data) throws Exception {

        int key = data.getHashCode();
        int index = getIndex(key);

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(primaryAddr);

        for (int i = 0; i < block.getValidCount(); i++) {
            T rec = block.getList().get(i);
            if (rec.isEqual(data)){
                return rec;
            }
        }

        long nextAddr = block.getNext();

        while (nextAddr != 0) {

            Block<T> ov = overflowFile.readBlock(nextAddr);

            for (int i = 0; i < ov.getValidCount(); i++) {
                T rec = ov.getList().get(i);
                if (rec.isEqual(data)) {
                    return rec;
                }
            }

            nextAddr = ov.getNext();
        }

        return null;
    }

    public boolean update(T pattern, T newRecord) throws Exception {

        int key = pattern.getHashCode();
        int index = getIndex(key);

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);

        // 1) UPDATE v PRIMÁRNOM BLOKU
        if (updateFromBlock(mainFile, primaryAddr, primary, pattern, newRecord)) {
            return true;
        }

        // 2) UPDATE v OVERFLOW BLOKOCH
        long nextAddr = primary.getNext();

        while (nextAddr != 0) {

            Block<T> ov = overflowFile.readBlock(nextAddr);

            if (updateFromBlock(overflowFile, nextAddr, ov, pattern, newRecord)) {
                return true;
            }

            nextAddr = ov.getNext();
        }

        return false;
    }


    public boolean delete(T pattern) throws Exception {

        int key = pattern.getHashCode();
        int index = getIndex(key);

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);

        // ==================================================
        // 1) POKUS O MAZANIE V PRIMÁRNOM BLOKU
        // ==================================================
        if (deleteFromBlock(mainFile, primaryAddr, primary, pattern)) {

            totalRecords--;
            recordCountPerIndex[index]--;

            if (getDensity() < d_min) merge();
            return true;
        }

        // ==================================================
        // 2) HĽADANIE V OVERFLOW REŤAZCI
        // ==================================================
        long nextAddr = primary.getNext();

        while (nextAddr != 0) {

            Block<T> ov = overflowFile.readBlock(nextAddr);

            if (deleteFromBlock(overflowFile, nextAddr, ov, pattern)) {

                totalRecords--;
                overflowChainLength[index]--;

                if (getDensity() < d_min) merge();
                return true;
            }

            nextAddr = ov.getNext();
        }

        return false;
    }

    //pomocna metoda kvoli duplicite
    private boolean deleteFromBlock(HeapFile<T> file, long addr, Block<T> block, T data) throws Exception {

        for (int i = 0; i < block.getValidCount(); i++) {

            T r = block.getList().get(i);

            if (r.isEqual(data)) {

                // shift left
                for (int j = i + 1; j < block.getValidCount(); j++) {
                    block.getList().set(j - 1, block.getList().get(j));
                }

                block.setValidCount(block.getValidCount() - 1);

                file.writeBlock(addr, block);

                return true;
            }
        }

        return false;
    }

    private boolean updateFromBlock(HeapFile<T> file, long addr, Block<T> block,
                                    T pattern, T newRecord) throws Exception {

        for (int i = 0; i < block.getValidCount(); i++) {

            T r = block.getList().get(i);

            if (r.isEqual(pattern)) {

                block.getList().set(i, newRecord);

                file.writeBlock(addr, block);

                return true;
            }
        }

        return false;
    }




    private int getIndex(int key) {

        int pow2 = (int) Math.pow(2, u);
        int range = M * pow2;

        int i = key % range;
        if (i < 0) i += range;

        if (i < S) {

            int extendedRange = M * (pow2 * 2);

            i = key % extendedRange;
            if (i < 0) i += extendedRange;
        }

        return i;
    }

    private double getDensity() {

        int currentGroups = this.S + this.M * (int) Math.pow(2, u);

        int totalCapacity = currentGroups * mainFile.getBlockFactor();

        if (totalCapacity == 0) return 0;

        return (double) totalRecords / totalCapacity;
    }

    private void split() throws Exception {

        int oldIndex = S;
        int newIndex = S + M * (int) Math.pow(2, u);

        long oldAddr = (long) oldIndex * mainFile.getBlockSize();
        long newAddr = (long) newIndex * mainFile.getBlockSize();

        Block<T> oldBlock = mainFile.readBlock(oldAddr);

        Block<T> blockOld = mainFile.createEmptyBlock();
        Block<T> blockNew = mainFile.createEmptyBlock();

        int divisor = M * (int) Math.pow(2, u + 1);

        for (int i = 0; i < oldBlock.getValidCount(); i++) {

            T record = oldBlock.getList().get(i);
            int key = record.getHashCode();

            int index = key % divisor;
            if (index < 0) index += divisor;

            if (index == oldIndex) {

                blockOld.getList().set(blockOld.getValidCount(), record);
                blockOld.setValidCount(blockOld.getValidCount() + 1);

            } else {

                blockNew.getList().set(blockNew.getValidCount(), record);
                blockNew.setValidCount(blockNew.getValidCount() + 1);
            }
        }

        mainFile.writeBlock(oldAddr, blockOld);
        mainFile.writeBlock(newAddr, blockNew);

        recordCountPerIndex[oldIndex] = blockOld.getValidCount();
        recordCountPerIndex[newIndex] = blockNew.getValidCount();

        S++;

        int groupsBefore = M * (int) Math.pow(2, u);

        if (S >= groupsBefore) {
            S = 0;
            u++;
        }
    }

    private void merge() throws Exception {

        int groupsBefore = M * (int) Math.pow(2, u);

        // =======================
        // PRÍPAD A) S > 0
        // =======================
        if (S > 0) {

            int lastIndex = S + groupsBefore - 1;
            int targetIndex = S - 1;

            mergeGroups(lastIndex, targetIndex);

            S--;
            return;
        }

        // =======================
        // PRÍPAD B) S == 0 a u > 0
        // =======================
        if (u > 0) {

            int lastIndex = groupsBefore - 1;

            u--; // znížime úroveň
            int groupsNow = M * (int) Math.pow(2, u);

            int targetIndex = groupsNow - 1;

            mergeGroups(lastIndex, targetIndex);

            S = targetIndex;  // reset S na koniec
        }
    }

    private void mergeGroups(int fromIndex, int toIndex) throws Exception {

        long fromAddr = (long) fromIndex * mainFile.getBlockSize();
        long toAddr   = (long) toIndex   * mainFile.getBlockSize();

        Block<T> from = mainFile.readBlock(fromAddr);
        Block<T> to   = mainFile.readBlock(toAddr);

        // presuň reálne záznamy z from → to
        for (int i = 0; i < from.getValidCount(); i++) {

            T record = from.getList().get(i);

            if (to.getValidCount() < mainFile.getBlockFactor()) {

                // zmestí sa do primárneho bloku
                to.getList().set(to.getValidCount(), record);
                to.setValidCount(to.getValidCount() + 1);
            }
            else {
                // vlož do overflow reťazca toIndex
                insertIntoOverflow(toIndex, record);
            }
        }

        // zapíš upravený blok target
        mainFile.writeBlock(toAddr, to);

        // clear from-block
        Block<T> empty = mainFile.createEmptyBlock();
        mainFile.writeBlock(fromAddr, empty);

        // štatistiky
        recordCountPerIndex[toIndex] = to.getValidCount();
        recordCountPerIndex[fromIndex] = 0;
    }








}





/*


    public boolean delete(T pattern) throws Exception {
        int key = pattern.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);

        for (int i = 0; i < block.getValidCount(); i++) {
            T r = block.getList().get(i);
            if (r.isEqual(pattern)) {

                for (int j = i+1; j < block.getValidCount(); j++) {
                    block.getList().set(j-1, block.getList().get(j));
                }

                block.setValidCount(block.getValidCount()-1);

                mainFile.writeBlockDirect(offset, block);

                totalRecords--;
                recordCountPerIndex[index]--;

                if (getDensity() < d_min) merge();
                return true;
            }
        }

        return false;
    }

    private void merge() throws Exception {
        System.out.println("MERGE: Spájam skupiny (S=" + S + ", u=" + u + ")");

        if (S > 0) {
            int lastIndex = S + M * (1 << u) - 1;
            int targetIndex = S - 1;

            mergeGroups(lastIndex, targetIndex);

            S--;

        } else if (u > 0) {
            int lastIndex = M * (1 << u) - 1;
            int targetIndex = M * (1 << (u - 1)) - 1;

            mergeGroups(lastIndex, targetIndex);

            u--;
            S = M * (1 << u) - 1;
        }
    }

    private void mergeGroups(int fromIndex, int toIndex) throws Exception {
        long fromAddr = (long) fromIndex * mainFile.getBlockSize();
        long toAddr = (long) toIndex * mainFile.getBlockSize();

        Block<T> fromBlock = mainFile.readBlock(fromAddr);
        Block<T> toBlock = mainFile.readBlock(toAddr);

        for (int i = 0; i < fromBlock.getValidCount(); i++) {
            T record = fromBlock.getList().get(i);

            if (toBlock.getValidCount() < mainFile.getBlockFactor()) {
                toBlock.getList().set(toBlock.getValidCount(), record);
                toBlock.setValidCount(toBlock.getValidCount() + 1);
            } else {
                System.out.println("WARNING: Merge - cieľový blok plný!");
            }
        }

        mainFile.writeBlockForTest(toAddr, toBlock);

        Block<T> emptyBlock = createEmptyBlock();
        mainFile.writeBlockForTest(fromAddr, emptyBlock);

        recordCountPerIndex[toIndex] = toBlock.getValidCount();
        recordCountPerIndex[fromIndex] = 0;
    }


    private T createDummy() {
        try {
            return (T) prototype.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Trieda musí mať prázdny konštruktor", e);
        }
    }

    private void saveMetadata() throws Exception {
        PrintWriter pw = new PrintWriter(metadataPath);

        pw.println(M);
        pw.println(u);
        pw.println(S);
        pw.println(d_max);
        pw.println(d_min);
        pw.println(totalRecords);

        int maxIndex = S + M * (1 << u);
        pw.println(maxIndex);
        for (int i = 0; i < maxIndex; i++) {
            pw.println(recordCountPerIndex[i]);
        }

        pw.close();
    }

    private void loadMetadata() throws Exception {
        File file = new File(metadataPath);
        if (!file.exists()) return;

        Scanner sc = new Scanner(file);

        M = Integer.parseInt(sc.nextLine());
        u = Integer.parseInt(sc.nextLine());
        S = Integer.parseInt(sc.nextLine());
        d_max = Double.parseDouble(sc.nextLine());
        d_min = Double.parseDouble(sc.nextLine());
        totalRecords = Integer.parseInt(sc.nextLine());

        int maxIndex = Integer.parseInt(sc.nextLine());
        for (int i = 0; i < maxIndex; i++) {
            recordCountPerIndex[i] = Integer.parseInt(sc.nextLine());
        }

        sc.close();
    }

    public void close() throws Exception {
        saveMetadata();
        mainFile.close();
        overflowFile.close();
    }

    public String print() {
        StringBuilder sb = new StringBuilder();

        try {
            sb.append("===== LINEAR HASH FILE =====\n");
            sb.append("M=").append(M).append(", u=").append(u).append(", S=").append(S).append("\n");
            sb.append("d_max=").append(d_max).append(", d_min=").append(d_min).append("\n");
            sb.append("Total records: ").append(totalRecords).append("\n");
            sb.append("Density: ").append(String.format("%.2f", getDensity())).append("\n\n");

            int maxIndex = S + M * (1 << u);

            sb.append("Main file:\n");
            for (int i = 0; i < maxIndex; i++) {
                long addr = (long) i * mainFile.getBlockSize();
                Block<T> block = mainFile.readBlock(addr);

                sb.append("Index ").append(i).append(" (addr=").append(addr).append("): ");
                sb.append("validCount=").append(block.getValidCount()).append(" [");

                for (int j = 0; j < block.getValidCount(); j++) {
                    if (j > 0) sb.append(", ");
                    sb.append(block.getList().get(j));
                }

                sb.append("]\n");
            }

        } catch (Exception e) {
            sb.append("ERROR: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }

    public int getM() { return M; }
    public int getU() { return u; }
    public int getS() { return S; }
    public int getTotalRecords() { return totalRecords; }*/

