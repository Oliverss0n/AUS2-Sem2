package DataStructures;

import java.io.*;
import java.util.*;

public class HeapFile<T extends IRecord<T>> {

    private RandomAccessFile raf;
    private String metaPath;
    private String path;

    private int blockSize;
    private int blockFactor;

    private ArrayList<Long> freeBlocks;
    private ArrayList<Long> partialBlocks;
    private T prototype;



    public HeapFile(String path, int blockSize, T prototype, boolean isOverflow) throws Exception {
        this.path = path;
        if (!isOverflow) {
            this.metaPath = path + ".meta";
        } else {
            this.metaPath = null;
        }

        this.blockSize = blockSize;
        this.prototype = prototype;

        this.blockFactor = (blockSize - 12) / prototype.getSize();

        this.freeBlocks = new ArrayList<>();
        this.partialBlocks = new ArrayList<>();

        this.raf = new RandomAccessFile(path, "rw");

        if (!isOverflow) {
            this.loadMetadata();
        }

    }



    private Block<T> emptyBlock() {
        ArrayList<T> list = new ArrayList<>(blockFactor);
        for (int i = 0; i < blockFactor; i++) {
            try {
                //list.add((T) prototype.getClass().newInstance());
                list.add(prototype.createEmpty());

            } catch (Exception e) {
                throw new RuntimeException("Trieda musi mat prazdny konstruktor", e);
            }
        }
        Block<T> block = new Block<>(blockFactor, prototype, list);
        block.setValidCount(0);
        block.setNext(-1);
        return block;
    }


    public Block<T> createEmptyBlock() {
        return emptyBlock();
    }


    public long insert(T data) throws Exception {

        long addr;

        if (!partialBlocks.isEmpty()) {
            Collections.sort(partialBlocks);
            addr = partialBlocks.get(0);

            Block<T> block = readBlock(addr);

            int validCount = block.getValidCount();
            block.getList().set(validCount, data);
            block.setValidCount(validCount + 1);

            if (block.getValidCount() == blockFactor)
                partialBlocks.remove(0);

            writeBlock(addr, block);
            return addr;
        }

        if (!freeBlocks.isEmpty()) {
            Collections.sort(freeBlocks);
            addr = freeBlocks.remove(0);

            Block<T> block = emptyBlock();
            block.getList().set(0, data);
            block.setValidCount(1);

            if (blockFactor > 1)
                partialBlocks.add(addr);

            writeBlock(addr, block);
            return addr;
        }

        addr = raf.length();

        Block<T> block = emptyBlock();
        block.getList().set(0, data);
        block.setValidCount(1);

        if (blockFactor > 1) {
            partialBlocks.add(addr);
        }

        writeBlock(addr, block);
        return addr;
    }

    public T get(long addr, T pattern) throws Exception {
        Block<T> block = readBlock(addr);

        for (int i = 0; i < block.getValidCount(); i++) {
            T pom = block.getList().get(i);
            if (pom.isEqual(pattern)){
                return pom;
            }
        }
        return null;
    }

    public boolean delete(long addr, T pattern) throws Exception {
        Block<T> block = readBlock(addr);
        int validCount = block.getValidCount();

        int index = -1;
        for (int i = 0; i < validCount; i++) {
            if (block.getList().get(i).isEqual(pattern)) {
                index = i;
                break;
            }
        }

        if (index == -1) {
            return false;
        }

        block.getList().set(index, block.getList().get(validCount - 1));
        block.setValidCount(validCount - 1);
        block.getList().set(block.getValidCount(), prototype.createEmpty());

        if (block.getValidCount() == 0) {
            handleEmptyBlock(addr);
            return true;
        }

        else if (!partialBlocks.contains(addr)){
            partialBlocks.add(addr);
        }

        writeBlock(addr, block);
        return true;
    }

    private void handleEmptyBlock(long addr) throws Exception {

        partialBlocks.remove(addr);

        long fileEnd = raf.length() - blockSize;

        if (addr == fileEnd) {
            if (!freeBlocks.contains(addr))
                freeBlocks.add(addr);

            shrinkFile();
        }
        else {
            if (!freeBlocks.contains(addr))
                freeBlocks.add(addr);
        }
    }


    public Block<T> readBlock(long addr) throws Exception {
        raf.seek(addr);
        byte[] arr = new byte[blockSize];
        raf.read(arr);

        ArrayList<Byte> byteList = new ArrayList<>(blockSize);
        for (byte bajt : arr) {
            byteList.add(bajt);
        }

        Block<T> block = emptyBlock();
        block.fromBytes(byteList);

        return block;
    }


    public void writeBlock(long addr, Block<T> block) throws Exception {
        ArrayList<Byte> byteList = block.getBytes();

        byte[] finalBytes = new byte[blockSize];

        int limit = Math.min(byteList.size(), blockSize);
        for (int i = 0; i < limit; i++) {
            finalBytes[i] = byteList.get(i);
        }


        raf.seek(addr);
        raf.write(finalBytes);
    }


    private void saveMetadata() throws Exception {
        if (metaPath == null) return;
        PrintWriter pw = new PrintWriter(metaPath);

        pw.println(blockFactor);
        pw.println(blockSize);

        pw.println(freeBlocks.size());
        for (long x : freeBlocks) {
            pw.println(x);
        }

        pw.println(partialBlocks.size());
        for (long x : partialBlocks) {
            pw.println(x);
        }

        pw.close();
    }

    private void loadMetadata() throws Exception {
        if (metaPath == null) return;
        File file = new File(metaPath);
        if (!file.exists()) return;

        Scanner sc = new Scanner(file);

        blockFactor = Integer.parseInt(sc.nextLine());
        blockSize = Integer.parseInt(sc.nextLine());

        int free = Integer.parseInt(sc.nextLine());
        freeBlocks.clear();
        for (int i = 0; i < free; i++) {
            freeBlocks.add(Long.parseLong(sc.nextLine()));
        }

        int part = Integer.parseInt(sc.nextLine());
        partialBlocks.clear();
        for (int i = 0; i < part; i++) {
            partialBlocks.add(Long.parseLong(sc.nextLine()));
        }

        sc.close();
    }

    public void close() throws Exception {
        saveMetadata();
        raf.close();
    }

    public String print() {
        StringBuilder sb = new StringBuilder();

        try {
            long fileLength = raf.length();
            long addr = 0;
            int blockIndex = 0;

            sb.append("File length: ").append(fileLength).append(" bytes\n");
            sb.append("Block size: ").append(blockSize).append(" bytes\n\n");

            while (addr + blockSize <= fileLength) {
                Block<T> block = readBlock(addr);

                if (block.getValidCount() > 0) {
                    sb.append("----- BLOCK ").append(blockIndex)
                            .append(" on address ").append(addr).append(" -----\n");
                    sb.append("validCount = ").append(block.getValidCount()).append("\n");

                    for (int i = 0; i < block.getValidCount(); i++) {
                        sb.append("  [").append(i).append("] ")
                                .append(block.getList().get(i)).append("\n");
                    }
                    sb.append("\n");
                }

                addr += blockSize;
                blockIndex++;
            }

            sb.append("Free blocks: ").append(freeBlocks).append("\n");
            sb.append("Semi-free blocks: ").append(partialBlocks).append("\n");

        } catch (Exception e) {
            sb.append("PRINT FAILED: ").append(e.getMessage()).append("\n");
        }

        return sb.toString();
    }

    public long writeNewBlock(Block<T> block) throws Exception {
        long addr = raf.length();
        writeBlock(addr, block);
        return addr;
    }


    public long getFileLength() throws Exception {
        return raf.length();
    }
    public int getBlockSize() {
        return blockSize;
    }


    public void shrinkFile() throws Exception {
        Collections.sort(freeBlocks);

        long newSize = raf.length();

        while (true) {
            long lastAddr = newSize - blockSize;
            if (lastAddr < 0) break;

            if (freeBlocks.contains(lastAddr)) {
                freeBlocks.remove(lastAddr);
                newSize -= blockSize;
            } else {
                break;
            }
        }

        raf.setLength(newSize);
    }


    public void addToFreeList(long address) {
        if (!freeBlocks.contains(address)) {
            freeBlocks.add(address);
        }
    }

    public int getBlockFactor() {
        return blockFactor;
    }



}