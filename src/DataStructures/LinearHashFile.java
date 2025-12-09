package DataStructures;

import java.io.*;
import java.util.*;

public class LinearHashFile<T extends IRecord<T>> {

    private boolean useDensity = false;
    private boolean useBucketSize = true;
    private boolean useOverflowCount = false;

    private double maxDensity = 1.3;
    private int maxBucketSize = 8;
    private int maxOverflowBlocks = 3;
    private ArrayList<Integer> bucketRecordCount;
    private ArrayList<Integer> bucketOverflowCount;

    private HeapFile<T> mainFile;
    private HeapFile<T> overflowFile;
    private int M;
    private int u;
    private int S;
    private int totalRecords;

    private String metadataPath;
    private T prototype;

    public LinearHashFile(String mainPath, int mainBlockSize, String overflowPath, int overflowBlockSize, int M, T prototype) throws Exception {

        this.prototype = prototype;

        this.M = M;
        this.u = 0;
        this.S = 0;

        this.totalRecords = 0;

        this.metadataPath = mainPath + ".lh.meta";
        bucketRecordCount = new ArrayList<>();
        bucketOverflowCount = new ArrayList<>();

        for (int i = 0; i < M; i++) {
            bucketRecordCount.add(0);
            bucketOverflowCount.add(0);
        }


        this.mainFile = new HeapFile<>(mainPath, mainBlockSize, prototype, false);
        this.overflowFile = new HeapFile<>(overflowPath, overflowBlockSize, prototype, true);

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

            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);
        }
    }


    public void insert(T record) throws Exception {
        int key = record.getHashCode();
        int index = getIndex(key);

        long offset = (long) index * mainFile.getBlockSize();
        Block<T> block = mainFile.readBlock(offset);

        boolean newOverflowBlock = false;

        if (block.getValidCount() < mainFile.getBlockFactor()) {

            block.getList().set(block.getValidCount(), record);
            block.setValidCount(block.getValidCount() + 1);

            mainFile.writeBlock(offset, block);


            bucketRecordCount.set(index, bucketRecordCount.get(index) + 1);

            totalRecords++;

        } else {

            newOverflowBlock = insertIntoOverflow(index, record);


            bucketRecordCount.set(index, bucketRecordCount.get(index) + 1);
            if (newOverflowBlock) {
                bucketOverflowCount.set(index,
                        bucketOverflowCount.get(index) + 1);
            }

            totalRecords++;
        }


        if (shouldSplit(index)) {
            split();
        }
    }


    private boolean insertIntoOverflow(int index, T record) throws Exception {

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);
        long nextAddr = primary.getNext();


        if (nextAddr == -1) {
            Block<T> newBlock = overflowFile.createEmptyBlock();
            newBlock.getList().set(0, record);
            newBlock.setValidCount(1);
            newBlock.setNext(-1);

            long newAddr = overflowFile.writeNewBlock(newBlock);

            primary.setNext(newAddr);
            mainFile.writeBlock(primaryAddr, primary);

            return true;
        }

        long currentAddr = nextAddr;
        Block<T> current = overflowFile.readBlock(currentAddr);

        while (true) {

            if (current.getValidCount() < overflowFile.getBlockFactor()) {

                current.getList().set(current.getValidCount(), record);
                current.setValidCount(current.getValidCount() + 1);

                overflowFile.writeBlock(currentAddr, current);

                return false;
            }

            if (current.getNext() == -1) {

                Block<T> newBlock = overflowFile.createEmptyBlock();
                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);
                newBlock.setNext(-1);

                long newAddr = overflowFile.writeNewBlock(newBlock);

                current.setNext(newAddr);
                overflowFile.writeBlock(currentAddr, current);

                return true;
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
            T record = block.getList().get(i);
            if (record.isEqual(data)){
                return record;
            }
        }

        long nextAddr = block.getNext();

        while (nextAddr != -1) {
            Block<T> overflowBlock = overflowFile.readBlock(nextAddr);

            for (int i = 0; i < overflowBlock.getValidCount(); i++) {
                T record = overflowBlock.getList().get(i);
                if (record.isEqual(data)) {
                    return record;
                }
            }

            nextAddr = overflowBlock.getNext();
        }

        return null;
    }


    public boolean update(T pattern, T newRecord) throws Exception {

        int key = pattern.getHashCode();
        int index = getIndex(key);

        long primaryAddr = (long) index * mainFile.getBlockSize();
        Block<T> primary = mainFile.readBlock(primaryAddr);

        if (updateFromBlock(mainFile, primaryAddr, primary, pattern, newRecord)) {
            return true;
        }

        long nextAddr = primary.getNext();

        while (nextAddr != -1) {

            Block<T> overflowBlock = overflowFile.readBlock(nextAddr);

            if (updateFromBlock(overflowFile, nextAddr, overflowBlock, pattern, newRecord)) {
                return true;
            }

            nextAddr = overflowBlock.getNext();
        }

        return false;
    }

    public boolean delete(T pattern) throws Exception{return false;}

    private boolean updateFromBlock(HeapFile<T> file, long addr, Block<T> block,
                                    T pattern, T newRecord) throws Exception {

        for (int i = 0; i < block.getValidCount(); i++) {

            T record = block.getList().get(i);

            if (record.isEqual(pattern)) {

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
        if (i < 0) {
            i += range;
        }

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

        int splitBucketIndex = S;
        int currentLevelSize = M * (int) Math.pow(2, u);
        int newBucketIndex = splitBucketIndex + currentLevelSize;
        int newDivisor = M * (int) Math.pow(2, u + 1);

        long blockSize = mainFile.getBlockSize();
        long oldBucketAddress = (long) splitBucketIndex * blockSize;
        long newBucketAddress = (long) newBucketIndex * blockSize;

        while (bucketRecordCount.size() <= newBucketIndex) {
            bucketRecordCount.add(0);
            bucketOverflowCount.add(0);
        }

        ArrayList<T> allRecordsFromBucket = new ArrayList<>();
        Block<T> oldPrimaryBlock = mainFile.readBlock(oldBucketAddress);

        for (int i = 0; i < oldPrimaryBlock.getValidCount(); i++) {
            allRecordsFromBucket.add(oldPrimaryBlock.getList().get(i));
        }

        ArrayList<Long> originalOverflowAddresses = new ArrayList<>();
        long currentOverflowAddress = oldPrimaryBlock.getNext();

        while (currentOverflowAddress != -1) {
            originalOverflowAddresses.add(currentOverflowAddress);
            Block<T> overflowBlock = overflowFile.readBlock(currentOverflowAddress);

            for (int i = 0; i < overflowBlock.getValidCount(); i++) {
                allRecordsFromBucket.add(overflowBlock.getList().get(i));
            }
            currentOverflowAddress = overflowBlock.getNext();
        }

        Block<T> newPrimaryForOldBucket = mainFile.createEmptyBlock();
        Block<T> newPrimaryForNewBucket = mainFile.createEmptyBlock();

        ArrayList<T> overflowRecordsForOldBucket = new ArrayList<>();
        ArrayList<T> overflowRecordsForNewBucket = new ArrayList<>();

        int primaryBlockFactor = mainFile.getBlockFactor();
        int overflowBlockFactor = overflowFile.getBlockFactor();

        for (T record : allRecordsFromBucket) {
            int targetBucketIndex = record.getHashCode() % newDivisor;
            if (targetBucketIndex < 0) targetBucketIndex += newDivisor;

            if (targetBucketIndex == splitBucketIndex) {
                if (newPrimaryForOldBucket.getValidCount() < primaryBlockFactor) {
                    newPrimaryForOldBucket.getList().set(newPrimaryForOldBucket.getValidCount(), record);
                    newPrimaryForOldBucket.setValidCount(newPrimaryForOldBucket.getValidCount() + 1);
                } else {
                    overflowRecordsForOldBucket.add(record);
                }
            } else {
                if (newPrimaryForNewBucket.getValidCount() < primaryBlockFactor) {
                    newPrimaryForNewBucket.getList().set(newPrimaryForNewBucket.getValidCount(), record);
                    newPrimaryForNewBucket.setValidCount(newPrimaryForNewBucket.getValidCount() + 1);
                } else {
                    overflowRecordsForNewBucket.add(record);
                }
            }
        }

        bucketRecordCount.set(splitBucketIndex, newPrimaryForOldBucket.getValidCount() + overflowRecordsForOldBucket.size());
        bucketRecordCount.set(newBucketIndex, newPrimaryForNewBucket.getValidCount() + overflowRecordsForNewBucket.size());

        bucketOverflowCount.set(splitBucketIndex, (overflowRecordsForOldBucket.size() + overflowBlockFactor - 1) / overflowBlockFactor);
        bucketOverflowCount.set(newBucketIndex, (overflowRecordsForNewBucket.size() + overflowBlockFactor - 1) / overflowBlockFactor);

        ArrayList<Block<T>> oldBucketOverflowBlocks = new ArrayList<>();
        ArrayList<Long> oldBucketOverflowAddresses = new ArrayList<>();

        ArrayList<Block<T>> newBucketOverflowBlocks = new ArrayList<>();
        ArrayList<Long> newBucketOverflowAddresses = new ArrayList<>();

        long overflowFileLength = overflowFile.getFileLength();
        int addressReuseIndex = 0;
        int additionalBlocksNeeded = 0;

        int recordPosition = 0;
        while (recordPosition < overflowRecordsForOldBucket.size()) {
            Block<T> overflowBlock = overflowFile.createEmptyBlock();
            int recordsInBlock = 0;

            while (recordsInBlock < overflowBlockFactor && recordPosition < overflowRecordsForOldBucket.size()) {
                overflowBlock.getList().set(recordsInBlock++, overflowRecordsForOldBucket.get(recordPosition++));
            }

            overflowBlock.setValidCount(recordsInBlock);
            overflowBlock.setNext(-1);

            long blockAddress = (addressReuseIndex < originalOverflowAddresses.size())
                    ? originalOverflowAddresses.get(addressReuseIndex++)
                    : overflowFileLength + (long) (additionalBlocksNeeded++) * blockSize;

            oldBucketOverflowBlocks.add(overflowBlock);
            oldBucketOverflowAddresses.add(blockAddress);
        }

        for (int i = 0; i < oldBucketOverflowBlocks.size() - 1; i++) {
            oldBucketOverflowBlocks.get(i).setNext(oldBucketOverflowAddresses.get(i + 1));
        }
        newPrimaryForOldBucket.setNext(oldBucketOverflowAddresses.isEmpty() ? -1 : oldBucketOverflowAddresses.get(0));

        recordPosition = 0;
        while (recordPosition < overflowRecordsForNewBucket.size()) {
            Block<T> overflowBlock = overflowFile.createEmptyBlock();
            int recordsInBlock = 0;

            while (recordsInBlock < overflowBlockFactor && recordPosition < overflowRecordsForNewBucket.size()) {
                overflowBlock.getList().set(recordsInBlock++, overflowRecordsForNewBucket.get(recordPosition++));
            }

            overflowBlock.setValidCount(recordsInBlock);
            overflowBlock.setNext(-1);

            long blockAddress = (addressReuseIndex < originalOverflowAddresses.size())
                    ? originalOverflowAddresses.get(addressReuseIndex++)
                    : overflowFileLength + (long) (additionalBlocksNeeded++) * blockSize;

            newBucketOverflowBlocks.add(overflowBlock);
            newBucketOverflowAddresses.add(blockAddress);
        }

        for (int i = 0; i < newBucketOverflowBlocks.size() - 1; i++) {
            newBucketOverflowBlocks.get(i).setNext(newBucketOverflowAddresses.get(i + 1));
        }
        newPrimaryForNewBucket.setNext(newBucketOverflowAddresses.isEmpty() ? -1 : newBucketOverflowAddresses.get(0));

        mainFile.writeBlock(oldBucketAddress, newPrimaryForOldBucket);
        mainFile.writeBlock(newBucketAddress, newPrimaryForNewBucket);

        for (int i = 0; i < oldBucketOverflowBlocks.size(); i++) {
            overflowFile.writeBlock(oldBucketOverflowAddresses.get(i), oldBucketOverflowBlocks.get(i));
        }
        for (int i = 0; i < newBucketOverflowBlocks.size(); i++) {
            overflowFile.writeBlock(newBucketOverflowAddresses.get(i), newBucketOverflowBlocks.get(i));
        }

        for (int i = addressReuseIndex; i < originalOverflowAddresses.size(); i++) {
            long address = originalOverflowAddresses.get(i);

            Block<T> emptyBlock = overflowFile.createEmptyBlock();
            emptyBlock.setValidCount(0);
            emptyBlock.setNext(-1);
            overflowFile.writeBlock(address, emptyBlock);

            overflowFile.addToFreeList(address);
        }

        S++;
        if (S >= currentLevelSize) {
            S = 0;
            u++;
        }

        overflowFile.shrinkFile();
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



    /*
    private void saveMetadata() throws Exception {

        PrintWriter pw = new PrintWriter(metadataPath);

        pw.println(M);
        pw.println(u);
        pw.println(S);
        pw.println(d_max);
        pw.println(totalRecords);


        pw.close();
    }*/

    private void saveMetadata() throws Exception {
        PrintWriter pw = new PrintWriter(metadataPath);

        pw.println(M);
        pw.println(u);
        pw.println(S);
        pw.println(maxDensity);
        pw.println(totalRecords);

        pw.println(bucketRecordCount.size());
        for (int count : bucketRecordCount) {
            pw.println(count);
        }

        pw.println(bucketOverflowCount.size());
        for (int count : bucketOverflowCount) {
            pw.println(count);
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
        maxDensity = Double.parseDouble(sc.nextLine());
        totalRecords = Integer.parseInt(sc.nextLine());

        int recordCountSize = Integer.parseInt(sc.nextLine());
        bucketRecordCount = new ArrayList<>();
        for (int i = 0; i < recordCountSize; i++) {
            bucketRecordCount.add(Integer.parseInt(sc.nextLine()));
        }

        int overflowCountSize = Integer.parseInt(sc.nextLine());
        bucketOverflowCount = new ArrayList<>();
        for (int i = 0; i < overflowCountSize; i++) {
            bucketOverflowCount.add(Integer.parseInt(sc.nextLine()));
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

            sb.append("----- OVERFLOW BLOCKS -----\n");

            long fileLen = overflowFile.getFileLength();
            long oBlockSize = overflowFile.getBlockSize();

            if (fileLen == 0) {
                sb.append("(no overflow blocks)\n");
                return sb.toString();
            }

            for (long addr = 0; addr + oBlockSize <= fileLen; addr += oBlockSize) {

                Block<T> overflowBlock = overflowFile.readBlock(addr);

                sb.append("Overflow @addr=").append(addr)
                        .append("  valid=").append(overflowBlock.getValidCount())
                        .append("  next=").append(overflowBlock.getNext())
                        .append("\n");

                for (int j = 0; j < overflowBlock.getValidCount(); j++) {
                    sb.append("    ").append(overflowBlock.getList().get(j)).append("\n");
                }
            }

            sb.append("=========================================\n");

        } catch (Exception e) {
            sb.append("ERROR IN PRINT: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }

    public void enableDensitySplit(double maxDensity) {
        this.useDensity = true;
        this.maxDensity = maxDensity;
    }

    public void enableBucketSizeSplit(int maxSize) {
        this.useBucketSize = true;
        this.maxBucketSize = maxSize;
    }

    public void enableOverflowCountSplit(int maxCount) {
        this.useOverflowCount = true;
        this.maxOverflowBlocks = maxCount;
    }

    private boolean shouldSplit(int index) {

        if (useDensity) {
            if (getDensity() > maxDensity) {
                return true;
            }
        }

        if (useBucketSize) {
            if (bucketRecordCount.get(index) > maxBucketSize) {
                return true;
            }
        }

        if (useOverflowCount) {
            if (bucketOverflowCount.get(index) > maxOverflowBlocks) {
                return true;
            }
        }

        return false;
    }





}

