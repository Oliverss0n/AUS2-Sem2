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

        int max = M * 1024; //!!!! prerobit
        this.recordCountPerIndex = new int[max];
        this.overflowChainLength = new int[max];

        this.mainFile = new HeapFile<>(mainPath, mainBlockSize, prototype);
        this.overflowFile = new HeapFile<>(overflowPath, overflowBlockSize, prototype);

        loadMetadata();
        if (mainFile.getFileLength() == 0) {
            createPrimaryBlocks();
        }

        if (overflowFile.getFileLength() == 0) {
            Block<T> dummy = overflowFile.createEmptyBlock();
            overflowFile.writeBlock(0, dummy);
            System.out.println("✅ Initialized overflow file with dummy block");
        }
    }

    /*
    private void createPrimaryBlocks() throws Exception {
        for (int i = 0; i < M; i++) {
            Block<T> empty = mainFile.createEmptyBlock();
            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);
        }
    }*/

    private void createPrimaryBlocks() throws Exception {
        System.out.println("\n🏗️ CREATING INITIAL PRIMARY BLOCKS:");
        for (int i = 0; i < M; i++) {
            Block<T> empty = mainFile.createEmptyBlock();
            System.out.println("   Block " + i + " created: next=" + empty.getNext());  // ✅

            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);

            // ✅ VERIFIKUJ PO ZÁPISE
            Block<T> verify = mainFile.readBlock(offset);
            System.out.println("   Block " + i + " verified: next=" + verify.getNext());

            if (verify.getNext() == 0) {
                throw new RuntimeException("FATAL: Initial block " + i + " has next=0!");
            }
        }
        System.out.println("✅ All primary blocks initialized\n");
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

            totalRecords++;
            recordCountPerIndex[index]++;

            if (getDensity() > d_max) split();
            return;
        }

        insertIntoOverflow(index, record);
    }*/
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
            totalRecords++;
            recordCountPerIndex[index]++;

            System.out.println("📝 Insert #" + totalRecords + ": density=" + String.format("%.4f", getDensity()) + ", d_max=" + d_max);  // ✅ PRIDAJ

            if (getDensity() > d_max) {
                System.out.println("🚨 TRIGGERING SPLIT!");  // ✅ PRIDAJ
                split();
            }
            return;
        }

        insertIntoOverflow(index, record);
    }
*/

    public void insert(T record) throws Exception {
        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);

        System.out.println("📝 Insert #" + (totalRecords + 1) +
                ": index=" + index +
                ", primaryValid=" + block.getValidCount() +
                "/" + mainFile.getBlockFactor() +
                ", currentNext=" + block.getNext());  // ✅ PRIDAJ

        if (block.getValidCount() < mainFile.getBlockFactor()) {
            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            System.out.println("   → Writing to primary, newValid=" + block.getValidCount() + ", next=" + block.getNext());  // ✅

            mainFile.writeBlock(offset, block);

            // ✅ VERIFIKUJ PO ZÁPISE
            Block<T> verify = mainFile.readBlock(offset);
            System.out.println("   → Verified after write: next=" + verify.getNext());
            if (verify.getNext() == 0) {
                throw new RuntimeException("PRIMARY BLOCK CORRUPTED at insert #" + (totalRecords + 1));
            }

            totalRecords++;
            recordCountPerIndex[index]++;

            if (getDensity() > d_max) split();
            return;
        }

        System.out.println("   → Primary full, going to overflow");
        insertIntoOverflow(index, record);
    }
    /*
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
    }*/

/*
    private void insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();

        if (nextAddr == -1) {
            Block<T> newBlock = overflowFile.createEmptyBlock();

            newBlock.getList().set(0, record);
            newBlock.setValidCount(1);
            newBlock.setNext(-1);         // 👈 istota

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

            if (current.getNext() == -1) {

                Block<T> newBlock = overflowFile.createEmptyBlock();

                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);
                newBlock.setNext(-1);        // 👈

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
*/

    private void insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();

        System.out.println("🔗 insertIntoOverflow: index=" + index + ", primary.next=" + nextAddr);

        // ==========================================
        // PRÍPAD 1: Žiadny overflow reťazec
        // ==========================================
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

            overflowChainLength[index]++;
            totalRecords++;

            if (getDensity() > d_max) split();  // ✅ PRIDAJ TOTO!
            return;
        }

        // ==========================================
        // PRÍPAD 2: Overflow reťazec už existuje
        // ==========================================
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
                // Koniec reťazca - vytvor nový blok
                System.out.println("   📦 End of chain, creating new overflow block");

                Block<T> newBlock = overflowFile.createEmptyBlock();
                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);
                newBlock.setNext(-1);

                long newAddr = overflowFile.writeNewBlock(newBlock);
                System.out.println("   ✅ Created overflow block @" + newAddr);

                // Link predchádzajúci blok
                current.setNext(newAddr);
                overflowFile.writeBlock(currentAddr, current);

                System.out.println("   🔗 Linked overflow@" + currentAddr + " -> overflow@" + newAddr);

                overflowChainLength[index]++;
                totalRecords++;

                if (getDensity() > d_max) split();  // ✅ PRIDAJ TOTO!
                return;
            }

            // Pokračuj na ďalší blok
            currentAddr = current.getNext();
            current = overflowFile.readBlock(currentAddr);
            System.out.println("   → Moving to next overflow@" + currentAddr);
        }
    }
/*
    private void insertIntoOverflow(int index, T record) throws Exception {
        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();

        System.out.println("🔗 insertIntoOverflow: index=" + index + ", primary.next=" + nextAddr);

        if (nextAddr == -1) {
            Block<T> newBlock = overflowFile.createEmptyBlock();
            newBlock.getList().set(0, record);
            newBlock.setValidCount(1);
            newBlock.setNext(-1);

            long newAddr = overflowFile.writeNewBlock(newBlock);
            System.out.println("   Created new overflow block @" + newAddr);

            // ✅ KRITICKÁ OPRAVA: Re-read primárny blok pred zápisom!
            primary = mainFile.readBlock(primaryAddr);  // 🔥 PRIDAJ TOTO
            primary.setNext(newAddr);

            System.out.println("   Setting primary.next=" + newAddr + " (was " + primary.getNext() + ")");

            mainFile.writeBlock(primaryAddr, primary);

            // ✅ VERIFIKUJ
            Block<T> verify = mainFile.readBlock(primaryAddr);
            System.out.println("   Verified: primary.next=" + verify.getNext());
            if (verify.getNext() != newAddr) {
                throw new RuntimeException("OVERFLOW LINK CORRUPTION!");
            }

            overflowChainLength[index]++;
            totalRecords++;
            return;
        }

 */


    /*
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
    }*/

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

/*
    public T find(T data) throws Exception {

        int key = data.getHashCode();
        int index = getIndex(key);

        // ==================================================
        // 1) ŠTANDARDNÉ HĽADANIE V DANOM BUCKETE
        //    (primárny blok + jeho overflow reťazec)
        // ==================================================
        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(primaryAddr);

        // primárny blok
        for (int i = 0; i < block.getValidCount(); i++) {
            T rec = block.getList().get(i);
            if (rec.isEqual(data)) {
                return rec;
            }
        }

        // overflow reťazec pre tento bucket
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

        // ==================================================
        // 2) FALLBACK – FULLSCAN CEZ CELÝ MAINFILE
        //    (ak je záznam omylom v "zlom" buckete)
        // ==================================================
        long fileLenMain = mainFile.getFileLength();
        long bSizeMain   = mainFile.getBlockSize();

        for (long addr = 0; addr + bSizeMain <= fileLenMain; addr += bSizeMain) {

            Block<T> b = mainFile.readBlock(addr);
            for (int i = 0; i < b.getValidCount(); i++) {
                T rec = b.getList().get(i);
                if (rec.isEqual(data)) {
                    return rec;
                }
            }
        }

        // ==================================================
        // 3) FALLBACK – FULLSCAN CEZ CELÝ OVERFLOW SÚBOR
        //    (zachytí aj "sirotské" overflow bloky ako addr=0)
        // ==================================================
        long fileLenOv = overflowFile.getFileLength();
        long bSizeOv   = overflowFile.getBlockSize();

        for (long addr = 0; addr + bSizeOv <= fileLenOv; addr += bSizeOv) {

            Block<T> ov = overflowFile.readBlock(addr);
            for (int i = 0; i < ov.getValidCount(); i++) {
                T rec = ov.getList().get(i);
                if (rec.isEqual(data)) {
                    return rec;
                }
            }
        }

        // nenašli sme
        return null;
    }*/

    /*
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
    }*/

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

        if (totalCapacity == 0) {
            return 0;
        }

        return (double) totalRecords / totalCapacity;
    }

/*
    private void split() throws Exception {

        System.out.println("\n=== SPLIT START ===");
        System.out.println("Splitting bucket S=" + S + ", u=" + u);

        // 1) Vypočítame indexy
        int oldIndex = S;
        int base     = M * (int) Math.pow(2, u);
        int newIndex = S + base;
        int divisor  = M * (int) Math.pow(2, u + 1);

        long blockSize = mainFile.getBlockSize();
        long oldAddr = (long) oldIndex * blockSize;
        long newAddr = (long) newIndex * blockSize;

        // 2) Načítame pôvodný primárny blok
        Block<T> oldPrimary = mainFile.readBlock(oldAddr);
        System.out.println("Old primary block at " + oldAddr + " has " + oldPrimary.getValidCount() + " records, next=" + oldPrimary.getNext());

        // 3) Zoberieme všetky záznamy z primárneho aj overflow reťazca
        ArrayList<T> allRecords = new ArrayList<>();

        // primárny blok
        for (int i = 0; i < oldPrimary.getValidCount(); i++) {
            allRecords.add(oldPrimary.getList().get(i));
        }

        // overflow reťazec pôvodného bucketu
        ArrayList<Long> overflowBlocks = new ArrayList<>();
        long ovAddr = oldPrimary.getNext();
        System.out.println("Collecting overflow blocks...");
        while (ovAddr != -1) {
            Block<T> ov = overflowFile.readBlock(ovAddr);
            overflowBlocks.add(ovAddr);

            for (int i = 0; i < ov.getValidCount(); i++) {
                allRecords.add(ov.getList().get(i));
            }

            ovAddr = ov.getNext();
        }

        System.out.println("Total records collected: " + allRecords.size());
        System.out.println("Overflow blocks to clear: " + overflowBlocks);


        // 5) Pripravíme nové prázdne primárne bloky
        Block<T> blockOld = mainFile.createEmptyBlock();
        blockOld.setValidCount(0);
        blockOld.setNext(-1);                 // 👈

        Block<T> blockNew = mainFile.createEmptyBlock();
        blockNew.setValidCount(0);
        blockNew.setNext(-1);                 // 👈


        // zoznamy pre overflow záznamy
        ArrayList<T> overflowOldRecs = new ArrayList<>();
        ArrayList<T> overflowNewRecs = new ArrayList<>();

        int primaryBF  = mainFile.getBlockFactor();
        int overflowBF = overflowFile.getBlockFactor();

        // 6) Znovu prehashujeme a rozdelíme všetky záznamy
        for (T rec : allRecords) {

            int key = rec.getHashCode();
            int idx = key % divisor;
            if (idx < 0) idx += divisor;

            if (idx == oldIndex) {
                // patrí do starého bucketu
                if (blockOld.getValidCount() < primaryBF) {
                    blockOld.getList().set(blockOld.getValidCount(), rec);
                    blockOld.setValidCount(blockOld.getValidCount() + 1);
                } else {
                    overflowOldRecs.add(rec);
                }
            } else {
                // patrí do nového bucketu
                if (blockNew.getValidCount() < primaryBF) {
                    blockNew.getList().set(blockNew.getValidCount(), rec);
                    blockNew.setValidCount(blockNew.getValidCount() + 1);
                } else {
                    overflowNewRecs.add(rec);
                }
            }
        }

        // 7) Vybudujeme OVERFLOW reťazec pre oldIndex
        long firstOverflowOld = -1;
        long prevAddrOld = -1;
        int usedOverflowBlocksOld = 0;

        int pos = 0;
        while (pos < overflowOldRecs.size()) {
            Block<T> ov = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < overflowBF && pos < overflowOldRecs.size()) {
                ov.getList().set(cnt, overflowOldRecs.get(pos));
                cnt++;
                pos++;
            }
            ov.setValidCount(cnt);
            ov.setNext(-1);

            long addr = overflowFile.writeNewBlock(ov);
            usedOverflowBlocksOld++;

            if (firstOverflowOld == -1) {
                firstOverflowOld = addr;
            } else {
                // pripneme na koniec predošlého
                Block<T> prev = overflowFile.readBlock(prevAddrOld);
                prev.setNext(addr);
                overflowFile.writeBlock(prevAddrOld, prev);
            }
            prevAddrOld = addr;
        }

        blockOld.setNext(firstOverflowOld);
        mainFile.writeBlock(oldAddr, blockOld);

        // 8) Vybudujeme OVERFLOW reťazec pre newIndex
        long firstOverflowNew = -1;
        long prevAddrNew = -1;
        int usedOverflowBlocksNew = 0;

        pos = 0;
        while (pos < overflowNewRecs.size()) {
            Block<T> ov = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < overflowBF && pos < overflowNewRecs.size()) {
                ov.getList().set(cnt, overflowNewRecs.get(pos));
                cnt++;
                pos++;
            }
            ov.setValidCount(cnt);
            ov.setNext(-1);

            long addr = overflowFile.writeNewBlock(ov);
            usedOverflowBlocksNew++;

            if (firstOverflowNew == -1) {
                firstOverflowNew = addr;
            } else {
                Block<T> prev = overflowFile.readBlock(prevAddrNew);
                prev.setNext(addr);
                overflowFile.writeBlock(prevAddrNew, prev);
            }
            prevAddrNew = addr;
        }

        blockNew.setNext(firstOverflowNew);
        mainFile.writeBlock(newAddr, blockNew);

        // 9) Aktualizujeme štatistiky
        int oldBucketCount = blockOld.getValidCount() + overflowOldRecs.size();
        int newBucketCount = blockNew.getValidCount() + overflowNewRecs.size();

        recordCountPerIndex[oldIndex] = oldBucketCount;
        recordCountPerIndex[newIndex] = newBucketCount;

        overflowChainLength[oldIndex] = usedOverflowBlocksOld;
        overflowChainLength[newIndex] = usedOverflowBlocksNew;

        System.out.println("Old bucket now has " + blockOld.getValidCount() + " primary, " + usedOverflowBlocksOld + " overflow blocks");
        System.out.println("New bucket now has " + blockNew.getValidCount() + " primary, " + usedOverflowBlocksNew + " overflow blocks");

        // 10) Posunieme S a u
        S++;
        int groupsBefore = base;
        if (S >= groupsBefore) {
            S = 0;
            u++;
        }

        // 11) Skráť overflow súbor
        System.out.println("Calling shrinkFileCompletely...");
        overflowFile.shrinkFileCompletely();
        System.out.println("Overflow file length after shrink: " + overflowFile.getFileLength());
        System.out.println("=== SPLIT END ===\n");
    }*/
/*
    private void split() throws Exception {

        int oldIndex = S;
        int base     = M * (int) Math.pow(2, u);
        int newIndex = oldIndex + base;
        int divisor  = M * (int) Math.pow(2, u + 1);

        long blockSize = mainFile.getBlockSize();
        long oldAddr   = (long) oldIndex * blockSize;
        long newAddr   = (long) newIndex * blockSize;

        // 1) Zožeň všetky záznamy starého bucketu
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

        // 2) Priprav prázdne primárne bloky
        Block<T> bOld = mainFile.createEmptyBlock();
        Block<T> bNew = mainFile.createEmptyBlock();
        bOld.setValidCount(0); bOld.setNext(-1);
        bNew.setValidCount(0); bNew.setNext(-1);

        ArrayList<T> ovOld = new ArrayList<>();
        ArrayList<T> ovNew = new ArrayList<>();

        int pBF = mainFile.getBlockFactor();
        int oBF = overflowFile.getBlockFactor();

        // 3) Rozdelenie záznamov podľa nového modula
        for (T r : all) {
            int idx = r.getHashCode() % divisor;
            if (idx < 0) idx += divisor;

            if (idx == oldIndex) {
                if (bOld.getValidCount() < pBF) {
                    bOld.getList().set(bOld.getValidCount(), r);
                    bOld.setValidCount(bOld.getValidCount() + 1);
                } else ovOld.add(r);
            } else {
                if (bNew.getValidCount() < pBF) {
                    bNew.getList().set(bNew.getValidCount(), r);
                    bNew.setValidCount(bNew.getValidCount() + 1);
                } else ovNew.add(r);
            }
        }

        // 4) Vybuduj overflow reťazec pre oldIndex
        long firstOld = -1, prev = -1;
        int pos = 0;
        while (pos < ovOld.size()) {
            Block<T> b = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < oBF && pos < ovOld.size()) {
                b.getList().set(cnt, ovOld.get(pos));
                cnt++; pos++;
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

        // 5) Overflow reťazec pre newIndex
        long firstNew = -1; prev = -1; pos = 0;
        while (pos < ovNew.size()) {
            Block<T> b = overflowFile.createEmptyBlock();
            int cnt = 0;
            while (cnt < oBF && pos < ovNew.size()) {
                b.getList().set(cnt, ovNew.get(pos));
                cnt++; pos++;
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

        // 6) Zapíš primárne bloky
        mainFile.writeBlock(oldAddr, bOld);
        mainFile.writeBlock(newAddr, bNew);

        // 7) Posuň S a u
        S++;
        if (S >= base) {
            S = 0;
            u++;
        }

        // 8) Zreže koniec overflow súboru
        overflowFile.shrinkFileCompletely();
    }
*/

    private void split() throws Exception {

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║         SPLIT START                    ║");
        System.out.println("╚════════════════════════════════════════╝");

        int oldIndex = S;
        int base = M * (int) Math.pow(2, u);
        int newIndex = oldIndex + base;
        int divisor = M * (int) Math.pow(2, u + 1);

        long blockSize = mainFile.getBlockSize();
        long oldAddr = (long) oldIndex * blockSize;
        long newAddr = (long) newIndex * blockSize;

        System.out.println("📊 SPLIT PARAMETERS:");
        System.out.println("   M=" + M + ", u=" + u + ", S=" + S);
        System.out.println("   oldIndex=" + oldIndex + " @addr=" + oldAddr);
        System.out.println("   newIndex=" + newIndex + " @addr=" + newAddr);
        System.out.println("   divisor=" + divisor);
        System.out.println("   mainFile.length=" + mainFile.getFileLength());

        // ==========================================
        // 1) Zbierka všetkých záznamov
        // ==========================================
        System.out.println("\n🔍 COLLECTING RECORDS FROM OLD BUCKET:");

        ArrayList<T> all = new ArrayList<>();
        Block<T> oldPrim = mainFile.readBlock(oldAddr);

        System.out.println("   Old primary: validCount=" + oldPrim.getValidCount() + ", next=" + oldPrim.getNext());

        for (int i = 0; i < oldPrim.getValidCount(); i++)
            all.add(oldPrim.getList().get(i));

        long ovAddr = oldPrim.getNext();
        int overflowCount = 0;
        while (ovAddr != -1) {
            Block<T> ov = overflowFile.readBlock(ovAddr);
            System.out.println("   Overflow block @" + ovAddr + ": validCount=" + ov.getValidCount() + ", next=" + ov.getNext());

            for (int i = 0; i < ov.getValidCount(); i++)
                all.add(ov.getList().get(i));

            overflowCount++;
            ovAddr = ov.getNext();
        }

        System.out.println("✅ Total records collected: " + all.size());
        System.out.println("✅ Overflow blocks traversed: " + overflowCount);

        // ==========================================
        // 2) Príprava prázdnych primárnych blokov
        // ==========================================
        System.out.println("\n🆕 CREATING NEW PRIMARY BLOCKS:");

        Block<T> bOld = mainFile.createEmptyBlock();
        Block<T> bNew = mainFile.createEmptyBlock();

        bOld.setValidCount(0);
        bOld.setNext(-1);
        bNew.setValidCount(0);
        bNew.setNext(-1);

        System.out.println("   bOld created: validCount=" + bOld.getValidCount() + ", next=" + bOld.getNext());
        System.out.println("   bNew created: validCount=" + bNew.getValidCount() + ", next=" + bNew.getNext());

        ArrayList<T> ovOld = new ArrayList<>();
        ArrayList<T> ovNew = new ArrayList<>();

        int pBF = mainFile.getBlockFactor();
        int oBF = overflowFile.getBlockFactor();

        // ==========================================
        // 3) Redistribúcia záznamov
        // ==========================================
        System.out.println("\n📦 REDISTRIBUTING RECORDS:");

        for (T r : all) {
            int idx = r.getHashCode() % divisor;
            if (idx < 0) idx += divisor;

            if (idx == oldIndex) {
                if (bOld.getValidCount() < pBF) {
                    bOld.getList().set(bOld.getValidCount(), r);
                    bOld.setValidCount(bOld.getValidCount() + 1);
                } else ovOld.add(r);
            } else {
                if (bNew.getValidCount() < pBF) {
                    bNew.getList().set(bNew.getValidCount(), r);
                    bNew.setValidCount(bNew.getValidCount() + 1);
                } else ovNew.add(r);
            }
        }

        System.out.println("   Old bucket: primary=" + bOld.getValidCount() + ", overflow=" + ovOld.size());
        System.out.println("   New bucket: primary=" + bNew.getValidCount() + ", overflow=" + ovNew.size());

        // ==========================================
        // 4) Overflow reťazec pre oldIndex
        // ==========================================
        System.out.println("\n🔗 BUILDING OVERFLOW CHAIN FOR OLD BUCKET:");

        long firstOld = -1, prev = -1;
        int pos = 0;
        int ovBlockCountOld = 0;

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
            ovBlockCountOld++;

            System.out.println("   Created overflow block #" + ovBlockCountOld + " @" + addr + " with " + cnt + " records");

            if (firstOld == -1) {
                firstOld = addr;
            } else {
                Block<T> p = overflowFile.readBlock(prev);
                p.setNext(addr);
                overflowFile.writeBlock(prev, p);
                System.out.println("   Linked: block@" + prev + " -> block@" + addr);
            }
            prev = addr;
        }

        bOld.setNext(firstOld);
        System.out.println("✅ Old overflow chain: firstAddr=" + firstOld + ", totalBlocks=" + ovBlockCountOld);

        // ==========================================
        // 5) Overflow reťazec pre newIndex
        // ==========================================
        System.out.println("\n🔗 BUILDING OVERFLOW CHAIN FOR NEW BUCKET:");

        long firstNew = -1;
        prev = -1;
        pos = 0;
        int ovBlockCountNew = 0;

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
            ovBlockCountNew++;

            System.out.println("   Created overflow block #" + ovBlockCountNew + " @" + addr + " with " + cnt + " records");

            if (firstNew == -1) {
                firstNew = addr;
            } else {
                Block<T> p = overflowFile.readBlock(prev);
                p.setNext(addr);
                overflowFile.writeBlock(prev, p);
                System.out.println("   Linked: block@" + prev + " -> block@" + addr);
            }
            prev = addr;
        }

        bNew.setNext(firstNew);
        System.out.println("✅ New overflow chain: firstAddr=" + firstNew + ", totalBlocks=" + ovBlockCountNew);

        // ==========================================
        // 6) Zápis primárnych blokov
        // ==========================================
        System.out.println("\n💾 WRITING PRIMARY BLOCKS:");
        System.out.println("   Writing OLD block @" + oldAddr + " (validCount=" + bOld.getValidCount() + ", next=" + bOld.getNext() + ")");
        System.out.println("   Writing NEW block @" + newAddr + " (validCount=" + bNew.getValidCount() + ", next=" + bNew.getNext() + ")");

        // 🔥 KRITICKÁ KONTROLA PRED ZÁPISOM
        if (bOld.getNext() == 0) {
            System.out.println("🔴🔴🔴 ERROR: bOld.next=0 BEFORE WRITE!");
            throw new RuntimeException("CRITICAL: bOld has next=0");
        }
        if (bNew.getNext() == 0) {
            System.out.println("🔴🔴🔴 ERROR: bNew.next=0 BEFORE WRITE!");
            throw new RuntimeException("CRITICAL: bNew has next=0");
        }

        mainFile.writeBlock(oldAddr, bOld);
        mainFile.writeBlock(newAddr, bNew);

        // ==========================================
        // 7) Verifikácia po zápise
        // ==========================================
        System.out.println("\n✅ VERIFICATION AFTER WRITE:");

        Block<T> verifyOld = mainFile.readBlock(oldAddr);
        Block<T> verifyNew = mainFile.readBlock(newAddr);

        System.out.println("   Read back OLD: validCount=" + verifyOld.getValidCount() + ", next=" + verifyOld.getNext());
        System.out.println("   Read back NEW: validCount=" + verifyNew.getValidCount() + ", next=" + verifyNew.getNext());

        if (verifyOld.getNext() == 0) {
            System.out.println("🔴🔴🔴 FATAL: OLD block has next=0 AFTER READ!");
            throw new RuntimeException("SPLIT CORRUPTION: old block next=0");
        }
        if (verifyNew.getNext() == 0) {
            System.out.println("🔴🔴🔴 FATAL: NEW block has next=0 AFTER READ!");
            throw new RuntimeException("SPLIT CORRUPTION: new block next=0");
        }

        // ==========================================
        // 8) Update S, u
        // ==========================================
        System.out.println("\n📈 UPDATING COUNTERS:");
        System.out.println("   Before: S=" + S + ", u=" + u);

        S++;
        if (S >= base) {
            S = 0;
            u++;
        }

        System.out.println("   After:  S=" + S + ", u=" + u);

        // ==========================================
        // 9) Cleanup
        // ==========================================
        System.out.println("\n🧹 CLEANUP:");
        overflowFile.shrinkFileCompletely();
        System.out.println("   Overflow file shrunk to: " + overflowFile.getFileLength());

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║         SPLIT END ✅                   ║");
        System.out.println("╚════════════════════════════════════════╝\n");
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

    private void tryShrinkOverflow(int index, long prevAddr, long emptyAddr) throws Exception {

        // prečítaj prázdny blok
        Block<T> empty = overflowFile.readBlock(emptyAddr);

        // je blok skutočne prázdny?
        if (empty.getValidCount() > 0) return;

        // overíme, či je emptyAddr posledný v súbore
        long lastBlockAddr = overflowFile.getFileLength() - overflowFile.getBlockSize();
        if (emptyAddr != lastBlockAddr) return;

        // MUSÍME odpojiť predchádzajúci blok
        if (prevAddr == -1) {   // 👈 žiadny predchádzajúci overflow
            long primaryAddr = (long) index * mainFile.getBlockSize();
            Block<T> primary = mainFile.readBlock(primaryAddr);

            primary.setNext(-1);                         // 👈
            mainFile.writeBlock(primaryAddr, primary);
        } else {
            Block<T> prev = overflowFile.readBlock(prevAddr);
            prev.setNext(-1);                            // 👈
            overflowFile.writeBlock(prevAddr, prev);
        }
        // fyzicky odrežeme blok zo súboru
        //overflowFile.shrinkFile();
        overflowFile.shrinkFileCompletely();
        // štatistika
        if (overflowChainLength[index] > 0)
            overflowChainLength[index]--;
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

        // základné parametre LH
        pw.println(M);
        pw.println(u);
        pw.println(S);
        pw.println(d_max);
        pw.println(d_min);
        pw.println(totalRecords);

        // počet aktuálnych skupín
        int groups = getCurrentGroups();
        pw.println(groups);

        // recordCountPerIndex
        for (int i = 0; i < groups; i++) {
            pw.println(recordCountPerIndex[i]);
        }

        // overflowChainLength
        for (int i = 0; i < groups; i++) {
            pw.println(overflowChainLength[i]);
        }

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

        // počet skupín keď sa LH predtým zatváral
        int groups = Integer.parseInt(sc.nextLine());

        // recordCountPerIndex
        for (int i = 0; i < groups; i++) {
            recordCountPerIndex[i] = Integer.parseInt(sc.nextLine());
        }

        // overflowChainLength
        for (int i = 0; i < groups; i++) {
            overflowChainLength[i] = Integer.parseInt(sc.nextLine());
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

            sb.append("=========== LINEAR HASH FILE ===========\n");
            sb.append("M = ").append(M)
                    .append(", u = ").append(u)
                    .append(", S = ").append(S).append("\n");
            sb.append("totalRecords = ").append(totalRecords).append("\n");
            sb.append("density = ").append(String.format("%.4f", getDensity())).append("\n");

            int currentGroups = S + M * (int)Math.pow(2, u);
            sb.append("current primary groups = ").append(currentGroups).append("\n\n");

            // ======================================
            // 1) PRIMÁRNE BLOKY
            // ======================================
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


