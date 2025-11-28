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

        //loadMetadata();

        if (mainFile.getFileLength() == 0) {
            createPrimaryBlocks();
        }
    }

    private void createPrimaryBlocks() throws Exception {
        for (int i = 0; i < M; i++) {
            Block<T> empty = mainFile.createEmptyBlock();
            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlockDirect(offset, empty);
        }
    }

    public void insert(T record) throws Exception {

        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlockForTest(offset);

        // ------------------------------------------
        // 1) POKÚSIME SA VLOŽIŤ DO PRIMÁRNEHO BLOKU
        // ------------------------------------------
        if (block.getValidCount() < mainFile.getBlockFactor()) {

            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            mainFile.writeBlockDirect(offset, block);

            totalRecords++;
            recordCountPerIndex[index]++;

            if (getDensity() > d_max) split();
            return;
        }

        // ------------------------------------------
        // 2) PRIMÁRNY JE PLNÝ -> VLOŽIŤ DO OVERFLOW
        // ------------------------------------------
        insertIntoOverflow(index, record);
    }

    private void insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlockForTest(primaryAddr);

        long next = primary.getNext();

        // ------------------------------------------
        // 1) AK ŽIADNY OVERFLOW BLOK NEEXISTUJE
        // ------------------------------------------
        if (next == 0) {

            long newAddr = overflowFile.insert(record);

            primary.setNext(newAddr);
            mainFile.writeBlockDirect(primaryAddr, primary);

            overflowChainLength[index]++;
            totalRecords++;
            return;
        }

        // ------------------------------------------
        // 2) EXISTUJE OVERFLOW BLOK -> HĽADÁME KONEC
        // ------------------------------------------
        long currentAddr = next;
        Block<T> current = overflowFile.readBlockForTest(currentAddr);

        while (true) {

            // ak je miesto v tomto bloku
            if (current.getValidCount() < overflowFile.getBlockFactor()) {

                current.getList().set(current.getValidCount(), record);
                current.setValidCount(current.getValidCount() + 1);

                overflowFile.writeBlockDirect(currentAddr, current);

                totalRecords++;
                return;
            }

            // ak už nemá ďalší -> vytvárame nový
            if (current.getNext() == 0) {

                long newAddr = overflowFile.insert(record);

                current.setNext(newAddr);
                overflowFile.writeBlockDirect(currentAddr, current);

                overflowChainLength[index]++;
                totalRecords++;

                return;
            }

            // posunieme sa na ďalší
            currentAddr = current.getNext();
            current = overflowFile.readBlockForTest(currentAddr);
        }
    }


    private int getIndex(int key) {

        // veľkosť súčasného rozsahu (M * 2^u)
        int pow2 = (int) Math.pow(2, u); //M*2^u
        int range = M * pow2;

        int i = key % range;

        if (i < 0){
            i += range;  // záporný hashCode fix
        }

        // ak index spadá do rozdelených skupín
        if (i < S) {

            // použijeme väčší rozsah (M * 2^(u+1))
            int pow2next = pow2 * 2;
            int extendedRange = M * pow2next;

            i = key % extendedRange;
            if (i < 0) i += extendedRange;
        }

        return i;
    }

    private double getDensity() {

        int currentGroups = this.S + this.M * (int)Math.pow(2, u);

        int totalCapacity = currentGroups * mainFile.getBlockFactor();

        if (totalCapacity == 0){
            return 0;
        }

        return (double) totalRecords / totalCapacity;
    }

    private void split() throws Exception {

        int oldIndex = S;
        int newIndex = S + M * (int) Math.pow(2, u);

        long oldAddr = (long) oldIndex * mainFile.getBlockSize();
        long newAddr = (long) newIndex * mainFile.getBlockSize();

        Block<T> oldBlock = mainFile.readBlockForTest(oldAddr);

        Block<T> blockOld = mainFile.createEmptyBlock();
        Block<T> blockNew = mainFile.createEmptyBlock();

        int divisor = M * (int) Math.pow(2, u + 1);

        for (int i = 0; i < oldBlock.getValidCount(); i++) {

            T record = oldBlock.getList().get(i);
            int key = record.getHashCode();

            int idx = key % divisor;
            if (idx < 0) idx += divisor;

            if (idx == oldIndex) {
                blockOld.getList().set(blockOld.getValidCount(), record);
                blockOld.setValidCount(blockOld.getValidCount() + 1);
            }
            else {
                blockNew.getList().set(blockNew.getValidCount(), record);
                blockNew.setValidCount(blockNew.getValidCount() + 1);
            }
        }

        mainFile.writeBlockForTest(oldAddr, blockOld);
        mainFile.writeBlockForTest(newAddr, blockNew);

        recordCountPerIndex[oldIndex] = blockOld.getValidCount();
        recordCountPerIndex[newIndex] = blockNew.getValidCount();

        S++;

        int groupsBefore = M * (int)Math.pow(2, u);

        if (S >= groupsBefore) {
            S = 0;
            u++;
        }
    }






/*
    private void initializeMainFile() throws Exception {
        for (int i = 0; i < M; i++) {
            T dummy = createDummy();
            long addr = mainFile.insert(dummy);
            mainFile.delete(addr, dummy);
        }
    }



    public T find(T pattern) throws Exception {
        int key = pattern.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlockForTest(offset);

        for (int i = 0; i < block.getValidCount(); i++) {
            T r = block.getList().get(i);
            if (r.isEqual(pattern)) return r;
        }

        return null;
    }

    public void insert(T record) throws Exception {
        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlockForTest(offset);

        if (block.getValidCount() < mainFile.getBlockFactor()) {
            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            mainFile.writeBlockDirect(offset, block);

            totalRecords++;
            recordCountPerIndex[index]++;

            if (getDensity() > d_max) split();
            return;
        }

        insertIntoOverflow(index, record);
    }

    public boolean delete(T pattern) throws Exception {
        int key = pattern.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlockForTest(offset);

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

    private void split() throws Exception {
        System.out.println("SPLIT: Rozdeľujem skupinu " + S + " (u=" + u + ")");

        long oldBlockAddr = (long) S * mainFile.getBlockSize();

        int newIndex = S + M * (1 << u);
        long newBlockAddr = (long) newIndex * mainFile.getBlockSize();

        Block<T> oldBlock = mainFile.readBlockForTest(oldBlockAddr);
        Block<T> newBlock = createEmptyBlock();
        Block<T> tempOld  = createEmptyBlock();

        for (int i = 0; i < oldBlock.getValidCount(); i++) {
            T record = oldBlock.getList().get(i);
            int rehashIndex = h_u_plus_1(record.getHashCode());

            if (rehashIndex == S) {
                tempOld.getList().set(tempOld.getValidCount(), record);
                tempOld.setValidCount(tempOld.getValidCount() + 1);
            } else {
                newBlock.getList().set(newBlock.getValidCount(), record);
                newBlock.setValidCount(newBlock.getValidCount() + 1);
            }
        }

        mainFile.writeBlockForTest(oldBlockAddr, tempOld);
        mainFile.writeBlockForTest(newBlockAddr, newBlock);

        recordCountPerIndex[S] = tempOld.getValidCount();
        recordCountPerIndex[newIndex] = newBlock.getValidCount();

        S++;

        if (S >= M * (1 << u)) {
            System.out.println("ÚPLNÁ EXPANZIA: u=" + u + " -> " + (u+1));
            S = 0;
            u++;
        }
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

        Block<T> fromBlock = mainFile.readBlockForTest(fromAddr);
        Block<T> toBlock = mainFile.readBlockForTest(toAddr);

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

    private double getDensity() {
        int currentBlocks = S + M * (1 << u);
        int totalCapacity = currentBlocks * mainFile.getBlockFactor();
        if (totalCapacity == 0) return 0;
        return (double) totalRecords / totalCapacity;
    }

    private Block<T> createEmptyBlock() {
        ArrayList<T> list = new ArrayList<>();
        for (int i = 0; i < mainFile.getBlockFactor(); i++) {
            list.add(createDummy());
        }
        return new Block<>(mainFile.getBlockFactor(), prototype, list);
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
                Block<T> block = mainFile.readBlockForTest(addr);

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
}
