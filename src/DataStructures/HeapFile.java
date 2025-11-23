package DataStructures;

import java.io.*;
import java.util.*;

public class HeapFile<T extends IRecord<T>> {

    private RandomAccessFile raf;
    private String path;
    private String metaPath;

    private int blockSize;
    private int blockFactor;

    private ArrayList<Long> freeBlocks;
    private ArrayList<Long> partialBlocks;
    private T prototype;

    // ============================================================
    // KONŠTRUKTOR
    // ============================================================
    public HeapFile(String path, int blockSize, T prototype) throws Exception {
        this.path = path;
        this.metaPath = path + ".meta";

        this.blockSize = blockSize;
        this.prototype = prototype;

        this.blockFactor = (blockSize - 4) / prototype.getSize();

        freeBlocks = new ArrayList<>();
        partialBlocks = new ArrayList<>();

        raf = new RandomAccessFile(path, "rw");

        loadMetadata();
    }

    // ============================================================
    // VYTVORÍ PRÁZDNY BLOK
    // ============================================================
    private Block<T> emptyBlock() {
        ArrayList<T> list = new ArrayList<>(blockFactor);
        for (int i = 0; i < blockFactor; i++) {
            try {
                list.add((T) prototype.getClass().newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Trieda musí mať prázdny konštruktor!");
            }
        }
        return new Block<>(blockFactor, prototype, list);
    }

    // ============================================================
    // INSERT
    // ============================================================
    public long insert(T data) throws Exception {

        long addr;

        // 1) partial block existuje
        if (!partialBlocks.isEmpty()) {
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

        // 2) free blok existuje
        if (!freeBlocks.isEmpty()) {
            addr = freeBlocks.remove(0);

            Block<T> block = emptyBlock();
            block.getList().set(0, data);
            block.setValidCount(1);

            if (blockFactor > 1)
                partialBlocks.add(addr);

            writeBlock(addr, block);
            return addr;
        }

        // 3) nový blok na konci súboru
        addr = raf.length();

        Block<T> b = emptyBlock();
        b.getList().set(0, data);
        b.setValidCount(1);

        if (blockFactor > 1)
            partialBlocks.add(addr);

        writeBlock(addr, b);
        return addr;
    }

    // ============================================================
    // GET
    // ============================================================
    public T get(long addr, T pattern) throws Exception {
        Block<T> block = readBlock(addr);

        for (int i = 0; i < block.getValidCount(); i++) {
            T x = block.getList().get(i);
            if (x.isEqual(pattern)) return x;
        }
        return null;
    }

    // ============================================================
    // DELETE
    // ============================================================
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

        if (index == -1) return false;

        block.getList().set(index, block.getList().get(validCount - 1));
        block.setValidCount(validCount - 1);

        if (block.getValidCount() == 0)
            handleEmptyBlock(addr);

        else if (!partialBlocks.contains(addr))
            partialBlocks.add(addr);

        writeBlock(addr, block);
        return true;
    }

    private void handleEmptyBlock(long addr) throws Exception {

        partialBlocks.remove(addr);

        long fileEnd = raf.length() - blockSize;

        if (addr == fileEnd) {
            shrinkFile();
        } else {
            freeBlocks.add(addr);
        }
    }


    // ============================================================
    // SKRÁTENIE SÚBORU
    // ============================================================
    private void shrinkFile() throws Exception {
        Collections.sort(freeBlocks);

        long newSize = raf.length();

        while (true) {
            long lastAddr = newSize - blockSize;
            if (lastAddr < 0) break;

            if (freeBlocks.contains(lastAddr)) {
                freeBlocks.remove(lastAddr);
                newSize -= blockSize;
            } else break;
        }

        raf.setLength(newSize);
    }

    // ============================================================
    // READ / WRITE BLOCK
    // ============================================================
    private Block<T> readBlock(long addr) throws Exception {
        byte[] buf = new byte[blockSize];

        raf.seek(addr);
        raf.read(buf);

        ArrayList<Byte> list = new ArrayList<>(blockSize);
        for (byte x : buf) list.add(x);

        Block<T> b = emptyBlock();
        b.fromBytes(list);
        return b;
    }

    private void writeBlock(long addr, Block<T> b) throws Exception {
        ArrayList<Byte> arr = b.getBytes();

        if (arr.size() > blockSize) {
            throw new RuntimeException("Block overflow: " + arr.size() + " > " + blockSize);
        }
        while (arr.size() < blockSize) {
            arr.add((byte)0);
        }

        byte[] raw = new byte[blockSize];
        for (int i = 0; i < blockSize; i++) raw[i] = arr.get(i);

        raf.seek(addr);
        raf.write(raw);
    }



    // ============================================================
    // METADARTA
    // ============================================================
    private void saveMetadata() throws Exception {
        PrintWriter pw = new PrintWriter(metaPath);

        pw.println(blockFactor);
        pw.println(blockSize);

        pw.println(freeBlocks.size());
        for (long x : freeBlocks) pw.println(x);

        pw.println(partialBlocks.size());
        for (long x : partialBlocks) pw.println(x);

        pw.close();
    }

    private void loadMetadata() throws Exception {
        File file = new File(metaPath);
        if (!file.exists()) return;

        Scanner sc = new Scanner(file);

        blockFactor = Integer.parseInt(sc.nextLine());
        blockSize   = Integer.parseInt(sc.nextLine());

        int free = Integer.parseInt(sc.nextLine());
        freeBlocks.clear();
        for (int i = 0; i < free; i++)
            freeBlocks.add(Long.parseLong(sc.nextLine()));

        int part = Integer.parseInt(sc.nextLine());
        partialBlocks.clear();
        for (int i = 0; i < part; i++)
            partialBlocks.add(Long.parseLong(sc.nextLine()));

        sc.close();
    }

    // ============================================================
    // CLOSE
    // ============================================================
    public void close() throws Exception {
        saveMetadata();
        raf.close();
    }


    // DEBUG: aby sme vedeli čítať blok zvonku
    public Block<T> readBlockForTest(long addr) throws Exception {
        return readBlock(addr);
    }

    public void print() {
        try {
            long fileLength = raf.length();

            System.out.println("======= HEAPFILE PRINT START =======");
            System.out.println("File length: " + fileLength + " bytes");
            System.out.println("DataStructure.Block size: " + blockSize + " bytes\n");

            long addr = 0;
            int blockIndex = 0;

            while (addr + blockSize <= fileLength) {

                Block<T> block = readBlock(addr);

                System.out.println("----- BLOCK " + blockIndex + " @ address " + addr + " -----");
                System.out.println("validCount = " + block.getValidCount());

                for (int i = 0; i < block.getValidCount(); i++) {
                    T rec = block.getList().get(i);
                    System.out.println(" [" + i + "] " + rec);
                }


                System.out.println();

                addr += blockSize;
                blockIndex++;
            }

            System.out.println("======= HEAPFILE PRINT END =======");

            System.out.println("Free blocks: " + freeBlocks);
            System.out.println("Semi-free blocks: " + partialBlocks);

        } catch (Exception e) {
            throw new RuntimeException("PRINT FAILED: " + e.getMessage(), e);
        }
    }

    public String printToString() {
        StringBuilder sb = new StringBuilder();

        try {
            long fileLength = raf.length();
            long addr = 0;
            int blockIndex = 0;

            sb.append("File length: ").append(fileLength).append(" bytes\n");
            sb.append("Block size: ").append(blockSize).append(" bytes\n\n");

            while (addr + blockSize <= fileLength) {
                Block<T> block = readBlock(addr);

                sb.append("----- BLOCK ").append(blockIndex)
                        .append(" @ address ").append(addr).append(" -----\n");
                sb.append("validCount = ").append(block.getValidCount()).append("\n");

                for (int i = 0; i < block.getValidCount(); i++) {
                    sb.append(" [").append(i).append("] ")
                            .append(block.getList().get(i)).append("\n");
                }
                sb.append("\n");

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


}
