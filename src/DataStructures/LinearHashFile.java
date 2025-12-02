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
    //private int[] recordCountPerIndex;
    //private int[] overflowChainLength;
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

        this.mainFile = new HeapFile<>(mainPath, mainBlockSize, prototype);
        this.overflowFile = new HeapFile<>(overflowPath, overflowBlockSize, prototype);

        loadMetadata();
        if (mainFile.getFileLength() == 0) {
            createPrimaryBlocks();
        }

        if (overflowFile.getFileLength() == 0) {
            Block<T> dummy = overflowFile.createEmptyBlock();
            overflowFile.writeBlock(0, dummy);
        }
    }



    private void createPrimaryBlocks() throws Exception {
        for (int i = 0; i < M; i++) {
            Block<T> empty = mainFile.createEmptyBlock();
            System.out.println("   Block " + i + " created: next=" + empty.getNext());

            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);

            Block<T> verify = mainFile.readBlock(offset);
            System.out.println("   Block " + i + " verified: next=" + verify.getNext());

            if (verify.getNext() == 0) {
                throw new RuntimeException("FATAL: Initial block " + i + " has next=0!");
            }
        }
    }


    /*
    public void insert(T record) throws Exception {
        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);

        if (block.getValidCount() < mainFile.getBlockFactor()) {
            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);


            mainFile.writeBlock(offset, block);

            Block<T> verify = mainFile.readBlock(offset);
            if (verify.getNext() == 0) {
                throw new RuntimeException("PRIMARY BLOCK CORRUPTED at insert #" + (totalRecords + 1));
            }

            totalRecords++;

            if (getDensity() > d_max) split();
            return;
        }


        insertIntoOverflow(index, record);
    }*/

    public void insert(T record) throws Exception {
        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);


        if (block.getValidCount() < mainFile.getBlockFactor()) {

            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            mainFile.writeBlock(offset, block);

            Block<T> verify = mainFile.readBlock(offset);

            if (verify.getNext() == 0) {
                throw new RuntimeException("PRIMARY BLOCK CORRUPTED at insert");
            }

            totalRecords++;

        } else {

            insertIntoOverflow(index, record);
            totalRecords++;
        }

        Block<T> realBlock = mainFile.readBlock(offset);
        int pBF = mainFile.getBlockFactor();

        int primaryCount = realBlock.getValidCount();

        int ovCount = 0;
        long ovPtr = realBlock.getNext();
        while (ovPtr != -1) {
            Block<T> ov = overflowFile.readBlock(ovPtr);
            ovCount += ov.getValidCount();
            ovPtr = ov.getNext();
        }

        int bucketCount = primaryCount + ovCount;

        if (bucketCount > 2 * pBF) {
            split();
        }
    }



    private void insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();

        if (nextAddr == -1) {
            Block<T> newBlock = overflowFile.createEmptyBlock();
            newBlock.getList().set(0, record);
            newBlock.setValidCount(1);
            newBlock.setNext(-1);

            long newAddr = overflowFile.writeNewBlock(newBlock);
            System.out.println("   ✅ Created first overflow block @" + newAddr);

            // 🔥 KRITICKÁ OPRAVA: Re-read primárny blok!
            primary = mainFile.readBlock(primaryAddr);
            primary.setNext(newAddr);

            System.out.println("   📌 Linking primary@" + primaryAddr + " -> overflow@" + newAddr);

            mainFile.writeBlock(primaryAddr, primary);

            // ✅ VERIFIKÁCIA
            Block<T> verify = mainFile.readBlock(primaryAddr);
            System.out.println("   ✔️ Verified: primary.next=" + verify.getNext());
            if (verify.getNext() != newAddr) {
                throw new RuntimeException("OVERFLOW LINK CORRUPTION at index " + index);
            }

            totalRecords++;

            if (getDensity() > d_max) split();  // ✅ PRIDAJ TOTO!
            return;
        }

        long currentAddr = nextAddr;
        Block<T> current = overflowFile.readBlock(currentAddr);

        System.out.println("   📂 Traversing existing overflow chain starting @" + currentAddr);

        while (true) {

            // Skús vložiť do aktuálneho bloku
            if (current.getValidCount() < overflowFile.getBlockFactor()) {
                System.out.println("   ✅ Found space in overflow@" + currentAddr + " (valid=" + current.getValidCount() + ")");

                current.getList().set(current.getValidCount(), record);
                current.setValidCount(current.getValidCount() + 1);

                overflowFile.writeBlock(currentAddr, current);

                totalRecords++;

                if (getDensity() > d_max) split();  // ✅ PRIDAJ TOTO!
                return;
            }

            // Aktuálny blok je plný, skús ďalší
            if (current.getNext() == -1) {

                Block<T> newBlock = overflowFile.createEmptyBlock();
                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);
                newBlock.setNext(-1);

                long newAddr = overflowFile.writeNewBlock(newBlock);

                // Link predchádzajúci blok
                current.setNext(newAddr);
                overflowFile.writeBlock(currentAddr, current);

                totalRecords++;

                if (getDensity() > d_max) split();
                return;
            }

            currentAddr = current.getNext();
            current = overflowFile.readBlock(currentAddr);
            System.out.println("   → Moving to next overflow@" + currentAddr);
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

        while (nextAddr != -1) {
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

        while (nextAddr != -1) {

            Block<T> ov = overflowFile.readBlock(nextAddr);

            if (updateFromBlock(overflowFile, nextAddr, ov, pattern, newRecord)) {
                return true;
            }

            nextAddr = ov.getNext();
        }

        return false;
    }

/*
    public boolean delete(T pattern) throws Exception {

        int key = pattern.getHashCode();
        int index = getIndex(key);

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);

        // ==================================================
        // 1) MAZANIE V PRIMÁRNOM BLOKU
        // ==================================================
        if (deleteFromBlock(mainFile, primaryAddr, primary, pattern)) {

            totalRecords--;
            recordCountPerIndex[index]--;

            if (getDensity() < d_min) merge();
            return true;
        }

        // ==================================================
        // 2) MAZANIE V OVERFLOW REŤAZCI
        // ==================================================
        long prevAddr = 0;
        long currentAddr = primary.getNext();

        while (currentAddr != -1) {

            Block<T> ov = overflowFile.readBlock(currentAddr);

            // pokus o zmazanie v tomto overflow bloku
            if (deleteFromBlock(overflowFile, currentAddr, ov, pattern)) {

                totalRecords--;

                // ak sa blok vyprázdnil → možno bude treba striasť
                if (ov.getValidCount() == 0) {
                    tryShrinkOverflow(index, prevAddr, currentAddr);
                }

                if (getDensity() < d_min) merge();
                return true;
            }

            // posun v reťazci
            prevAddr = currentAddr;
            currentAddr = ov.getNext();
        }

        return false;
    }
*/
    public boolean delete(T pattern) throws Exception{return false;}
    //pomocna metoda kvoli duplicite - este ju treba upravit
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

        if (totalCapacity == 0) {
            return 0;
        }

        return (double) totalRecords / totalCapacity;
    }

    private void split() throws Exception {

        int oldIndex = S;
        int base     = M * (int) Math.pow(2, u);
        int newIndex = oldIndex + base;
        int divisor  = M * (int) Math.pow(2, u + 1);

        long blockSize = mainFile.getBlockSize();
        long oldAddr   = (long) oldIndex * blockSize;
        long newAddr   = (long) newIndex * blockSize;

        ArrayList<T> all = new ArrayList<>();
        Block<T> oldPrim = mainFile.readBlock(oldAddr);

        for (int i = 0; i < oldPrim.getValidCount(); i++)
            all.add(oldPrim.getList().get(i));

        long ovAddr = oldPrim.getNext();
        while (ovAddr != -1) {
            Block<T> ov = overflowFile.readBlock(ovAddr);
            for (int i = 0; i < ov.getValidCount(); i++)
                all.add(ov.getList().get(i));
            ovAddr = ov.getNext();
        }

        Block<T> bOld = mainFile.createEmptyBlock();
        Block<T> bNew = mainFile.createEmptyBlock();
        bOld.setNext(-1);
        bNew.setNext(-1);

        ArrayList<T> ovOld = new ArrayList<>();
        ArrayList<T> ovNew = new ArrayList<>();

        int pBF = mainFile.getBlockFactor();
        int oBF = overflowFile.getBlockFactor();

        for (T r : all) {
            int idx = r.getHashCode() % divisor;
            if (idx < 0) idx += divisor;

            if (idx == oldIndex) {
                if (bOld.getValidCount() < pBF) {
                    bOld.getList().set(bOld.getValidCount(), r);
                    bOld.setValidCount(bOld.getValidCount() + 1);
                } else {
                    ovOld.add(r);
                }
            } else {
                if (bNew.getValidCount() < pBF) {
                    bNew.getList().set(bNew.getValidCount(), r);
                    bNew.setValidCount(bNew.getValidCount() + 1);
                } else {
                    ovNew.add(r);
                }
            }
        }

        long firstOld = -1;
        long prev = -1;
        int pos = 0;

        while (pos < ovOld.size()) {
            Block<T> b = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < oBF && pos < ovOld.size()) {
                b.getList().set(cnt, ovOld.get(pos));
                cnt++;
                pos++;
            }
            b.setValidCount(cnt);
            b.setNext(-1);

            long addr = overflowFile.writeNewBlock(b);

            if (firstOld == -1) firstOld = addr;
            else {
                Block<T> p = overflowFile.readBlock(prev);
                p.setNext(addr);
                overflowFile.writeBlock(prev, p);
            }
            prev = addr;
        }
        bOld.setNext(firstOld);

        long firstNew = -1;
        prev = -1;
        pos = 0;

        while (pos < ovNew.size()) {
            Block<T> b = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < oBF && pos < ovNew.size()) {
                b.getList().set(cnt, ovNew.get(pos));
                cnt++;
                pos++;
            }
            b.setValidCount(cnt);
            b.setNext(-1);

            long addr = overflowFile.writeNewBlock(b);

            if (firstNew == -1) firstNew = addr;
            else {
                Block<T> p = overflowFile.readBlock(prev);
                p.setNext(addr);
                overflowFile.writeBlock(prev, p);
            }
            prev = addr;
        }
        bNew.setNext(firstNew);

        mainFile.writeBlock(oldAddr, bOld);
        mainFile.writeBlock(newAddr, bNew);

        S++;
        if (S >= base) {
            S = 0;
            u++;
        }

        overflowFile.shrinkFileCompletely();
    }

    private int getCurrentGroups() {
        return S + M * (int)Math.pow(2, u);
    }

    public int getM() {
        return M;
    }

    public int getU() {
        return u;
    }

    public int getS() {
        return S;
    }

    public HeapFile<T> getMainFile() {
        return mainFile;
    }

    public HeapFile<T> getOverflowFile() {
        return overflowFile;
    }

    private void saveMetadata() throws Exception {

        PrintWriter pw = new PrintWriter(metadataPath);

        pw.println(M);
        pw.println(u);
        pw.println(S);
        pw.println(d_max);
        pw.println(d_min);
        pw.println(totalRecords);

        int groups = getCurrentGroups();
        pw.println(groups);

        pw.close();
    }


    private void loadMetadata() throws Exception {

        File f = new File(metadataPath);
        if (!f.exists()) return;

        Scanner sc = new Scanner(f);

        M = Integer.parseInt(sc.nextLine());
        u = Integer.parseInt(sc.nextLine());
        S = Integer.parseInt(sc.nextLine());
        d_max = Double.parseDouble(sc.nextLine());
        d_min = Double.parseDouble(sc.nextLine());
        totalRecords = Integer.parseInt(sc.nextLine());


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

            sb.append("=========== LINEAR HASH FILE ===========\n");
            sb.append("M = ").append(M)
                    .append(", u = ").append(u)
                    .append(", S = ").append(S).append("\n");
            sb.append("totalRecords = ").append(totalRecords).append("\n");
            sb.append("density = ").append(String.format("%.4f", getDensity())).append("\n");

            int currentGroups = S + M * (int)Math.pow(2, u);
            sb.append("current primary groups = ").append(currentGroups).append("\n\n");

            sb.append("----- PRIMARY BLOCKS -----\n");

            long blockSize = mainFile.getBlockSize();

            for (int i = 0; i < currentGroups; i++) {

                long addr = (long) i * blockSize;
                Block<T> block = mainFile.readBlock(addr);

                sb.append("Index ").append(i)
                        .append(" @addr=").append(addr)
                        .append("  valid=").append(block.getValidCount())
                        .append("  next=").append(block.getNext())
                        .append("\n");

                for (int j = 0; j < block.getValidCount(); j++) {
                    sb.append("    ").append(block.getList().get(j)).append("\n");
                }
            }

            sb.append("\n");

            // ======================================
            // 2) OVERFLOW BLOKY
            // ======================================
            sb.append("----- OVERFLOW BLOCKS -----\n");

            long fileLen = overflowFile.getFileLength();
            long oBlockSize = overflowFile.getBlockSize();

            if (fileLen == 0) {
                sb.append("(no overflow blocks)\n");
                return sb.toString();
            }

            for (long addr = 0; addr + oBlockSize <= fileLen; addr += oBlockSize) {

                Block<T> ov = overflowFile.readBlock(addr);

                sb.append("Overflow @addr=").append(addr)
                        .append("  valid=").append(ov.getValidCount())
                        .append("  next=").append(ov.getNext())
                        .append("\n");

                for (int j = 0; j < ov.getValidCount(); j++) {
                    sb.append("    ").append(ov.getList().get(j)).append("\n");
                }
            }

            sb.append("=========================================\n");

        } catch (Exception e) {
            sb.append("ERROR IN PRINT: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }

}


