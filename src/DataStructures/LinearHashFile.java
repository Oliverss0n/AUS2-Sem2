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
    private int totalRecords;

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

            long offset = (long) i * mainFile.getBlockSize();
            mainFile.writeBlock(offset, empty);
        }
    }


    /* s defaultnou kontroloou
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
            Block<T> overflowBlock = overflowFile.readBlock(ovPtr);
            ovCount += overflowBlock.getValidCount();
            ovPtr = overflowBlock.getNext();
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

            primary = mainFile.readBlock(primaryAddr);
            primary.setNext(newAddr);

            mainFile.writeBlock(primaryAddr, primary);

            //totalRecords++;

            //if (getDensity() > d_max) split();
            return;
        }

        long currentAddr = nextAddr;
        Block<T> current = overflowFile.readBlock(currentAddr);

        while (true) {

            if (current.getValidCount() < overflowFile.getBlockFactor()) {

                current.getList().set(current.getValidCount(), record);
                current.setValidCount(current.getValidCount() + 1);

                overflowFile.writeBlock(currentAddr, current);

                //totalRecords++;

                //if (getDensity() > d_max) split();
                return;
            }

            if (current.getNext() == -1) {

                Block<T> newBlock = overflowFile.createEmptyBlock();
                newBlock.getList().set(0, record);
                newBlock.setValidCount(1);
                newBlock.setNext(-1);

                long newAddr = overflowFile.writeNewBlock(newBlock);

                current.setNext(newAddr);
                overflowFile.writeBlock(currentAddr, current);

                //totalRecords++;

               // if (getDensity() > d_max) split();
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

    //este pre defaultnu podmienku
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

        ArrayList<T> allRecords = new ArrayList<>();
        Block<T> oldPrim = mainFile.readBlock(oldAddr);

        //nazbieranie starych zaznamov
        for (int i = 0; i < oldPrim.getValidCount(); i++) {
            allRecords.add(oldPrim.getList().get(i));
        }

        long ovFlowAddr = oldPrim.getNext();
        while (ovFlowAddr != -1) {
            Block<T> overflowBlock = overflowFile.readBlock(ovFlowAddr);
            for (int i = 0; i < overflowBlock.getValidCount(); i++) {
                allRecords.add(overflowBlock.getList().get(i));
            }
            ovFlowAddr = overflowBlock.getNext();
        }

        //vytvorenie novych blockov
        Block<T> primaryOld = mainFile.createEmptyBlock();
        Block<T> primaryNew = mainFile.createEmptyBlock();
        primaryOld.setNext(-1);
        primaryNew.setNext(-1);

        ArrayList<T> ovFlowOld = new ArrayList<>();
        ArrayList<T> ovFlowNew = new ArrayList<>();

        int pBF = mainFile.getBlockFactor();
        int oBF = overflowFile.getBlockFactor();

        //roztriedenie zaznamov medzi stary a novy
        for (T record : allRecords) {
            int hashIndex = record.getHashCode() % divisor;
            if (hashIndex < 0) {
                hashIndex += divisor;
            }

            if (hashIndex == oldIndex) {
                if (primaryOld.getValidCount() < pBF) {
                    primaryOld.getList().set(primaryOld.getValidCount(), record);
                    primaryOld.setValidCount(primaryOld.getValidCount() + 1);
                } else {
                    ovFlowOld.add(record);
                }
            } else {
                if (primaryNew.getValidCount() < pBF) {
                    primaryNew.getList().set(primaryNew.getValidCount(), record);
                    primaryNew.setValidCount(primaryNew.getValidCount() + 1);
                } else {
                    ovFlowNew.add(record);
                }
            }
        }

        long firstBlockAddr = -1;
        long previousBlockAddr = -1;
        int recordPosition = 0; //alebo index v arrayliste

        //overflow pre povodny block
        while (recordPosition < ovFlowOld.size()) {
            Block<T> ovFlowBlock = overflowFile.createEmptyBlock();
            int recordCount = 0;

            while (recordCount < oBF && recordPosition < ovFlowOld.size()) {
                ovFlowBlock.getList().set(recordCount, ovFlowOld.get(recordPosition));
                recordCount++;
                recordPosition++;
            }

            ovFlowBlock.setValidCount(recordCount);
            ovFlowBlock.setNext(-1);

            long addr = overflowFile.writeNewBlock(ovFlowBlock);

            if (firstBlockAddr == -1) {
                firstBlockAddr = addr;
            }
            else {
                Block<T> previousBlock = overflowFile.readBlock(previousBlockAddr);
                previousBlock.setNext(addr);
                overflowFile.writeBlock(previousBlockAddr, previousBlock);
            }
            previousBlockAddr = addr;
        }
        primaryOld.setNext(firstBlockAddr);

        long firstNew = -1;
        previousBlockAddr = -1;
        recordPosition = 0;

        ////overflow pre novy block
        while (recordPosition < ovFlowNew.size()) {
            Block<T> ovFlowBlock = overflowFile.createEmptyBlock();
            int recordCount = 0;

            while (recordCount < oBF && recordPosition < ovFlowNew.size()) {
                ovFlowBlock.getList().set(recordCount, ovFlowNew.get(recordPosition));
                recordCount++;
                recordPosition++;
            }

            ovFlowBlock.setValidCount(recordCount);
            ovFlowBlock.setNext(-1);

            long addr = overflowFile.writeNewBlock(ovFlowBlock);

            if (firstNew == -1) {
                firstNew = addr;
            }
            else {
                Block<T> previousBlock = overflowFile.readBlock(previousBlockAddr);
                previousBlock.setNext(addr);
                overflowFile.writeBlock(previousBlockAddr, previousBlock);
            }
            previousBlockAddr = addr;
        }
        primaryNew.setNext(firstNew);

        mainFile.writeBlock(oldAddr, primaryOld);
        mainFile.writeBlock(newAddr, primaryNew);

        S++;
        if (S >= base) {
            S = 0;
            u++;
        }

        overflowFile.shrinkFileLH();
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
        pw.println(totalRecords);


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

}

