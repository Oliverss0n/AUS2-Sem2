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

        while (currentAddr != 0) {

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
            if (index < 0) {
                index += divisor;
            }

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

    private void tryShrinkOverflow(int index, long prevAddr, long emptyAddr) throws Exception {

        // prečítaj prázdny blok
        Block<T> empty = overflowFile.readBlock(emptyAddr);

        // je blok skutočne prázdny?
        if (empty.getValidCount() > 0) return;

        // overíme, či je emptyAddr posledný v súbore
        long lastBlockAddr = overflowFile.getFileLength() - overflowFile.getBlockSize();
        if (emptyAddr != lastBlockAddr) return; // nie je posledný → nestriasame

        // MUSÍME odpojiť predchádzajúci blok
        if (prevAddr == 0) {
            // prázdny bol prvý overflow blok priamo za primárnym
            long primaryAddr = (long) index * mainFile.getBlockSize();
            Block<T> primary = mainFile.readBlock(primaryAddr);

            primary.setNext(0);
            mainFile.writeBlock(primaryAddr, primary);
        } else {
            // prázdny bol nejaký ďalší v reťazci
            Block<T> prev = overflowFile.readBlock(prevAddr);
            prev.setNext(0);
            overflowFile.writeBlock(prevAddr, prev);
        }

        // fyzicky odrežeme blok zo súboru
        overflowFile.shrinkFile();

        // štatistika
        if (overflowChainLength[index] > 0)
            overflowChainLength[index]--;
    }









}


