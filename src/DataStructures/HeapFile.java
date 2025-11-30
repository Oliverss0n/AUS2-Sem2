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

    public HeapFile(String path, int blockSize, T prototype) throws Exception {
        this.path = path;
        this.metaPath = path + ".meta";

        this.blockSize = blockSize;
        this.prototype = prototype;

        this.blockFactor = (blockSize - 4) / prototype.getSize();

        this.freeBlocks = new ArrayList<>();
        this.partialBlocks = new ArrayList<>();

        this.raf = new RandomAccessFile(path, "rw");

        this.loadMetadata();
    }


    private Block<T> emptyBlock() {
        ArrayList<T> list = new ArrayList<>(blockFactor);
        for (int i = 0; i < blockFactor; i++) {
            try {
                list.add((T) prototype.getClass().newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Trieda musi mat prazdny konstruktor", e);
            }
        }
        return new Block<>(blockFactor, prototype, list);
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



/*
    public Block<T> readBlock(long addr) throws Exception {
        byte[] buf = new byte[this.blockSize];

        raf.seek(addr);
        raf.read(buf);

        ArrayList<Byte> list = new ArrayList<>(this.blockSize);
        for (byte prvok : buf) {
            list.add(prvok);
        }

        Block<T> block = emptyBlock();
        block.fromBytes(list);
        return block;
    }*/
public Block<T> readBlock(long addr) throws Exception {

    raf.seek(addr);
    byte[] raw = new byte[blockSize];
    raf.read(raw);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));

    Block<T> block = emptyBlock();

    // 1) read validCount
    block.setValidCount(dis.readInt());

    // 2) read next pointer
    block.setNext(dis.readLong());

    // 3) read all T records
    for (int i = 0; i < blockFactor; i++) {
        ArrayList<Byte> bytes = new ArrayList<>();
        for (int j = 0; j < prototype.getSize(); j++) {
            bytes.add(dis.readByte());
        }
        T rec = (T) prototype.getClass().newInstance();
        rec.fromBytes(bytes);
        block.getList().set(i, rec);
    }

    return block;
}


    /*
    public void writeBlock(long addr, Block<T> block) throws Exception {
        ArrayList<Byte> arr = block.getBytes();

        while (arr.size() < this.blockSize) {
            arr.add((byte)0);
        }

        byte[] raw = new byte[this.blockSize];
        for (int i = 0; i < this.blockSize; i++) {
            raw[i] = arr.get(i);
        }

        raf.seek(addr);
        raf.write(raw);
    } */

    public void writeBlock(long addr, Block<T> block) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // 1) write valid count
        dos.writeInt(block.getValidCount());

        // 2) write next pointer
        dos.writeLong(block.getNext());

        // 3) write all T records
        for (int i = 0; i < blockFactor; i++) {
            T rec = block.getList().get(i);
            ArrayList<Byte> bytes = rec.getBytes();
            for (byte b : bytes) {
                dos.writeByte(b);
            }
        }

        byte[] raw = baos.toByteArray();

        // pad to full blockSize
        while (raw.length < blockSize) {
            baos.write(0);
            raw = baos.toByteArray();
        }

        raf.seek(addr);
        raf.write(raw);
    }



    //este neotestovane, len napad na implementaciu
    private void saveMetadata() throws Exception {
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
        File file = new File(metaPath);
        if (!file.exists()) return;

        Scanner sc = new Scanner(file);

        blockFactor = Integer.parseInt(sc.nextLine());
        blockSize   = Integer.parseInt(sc.nextLine());

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
        // ✅ VŽDY pridaj na koniec súboru, IGNORUJ freeBlocks
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


    public void writeBlockForTest(long addr, Block<T> block) throws Exception {
        writeBlock(addr, block);
    }


    public void freeBlock(long addr) throws Exception {
        Block<T> empty = createEmptyBlock();
        empty.setValidCount(0);
        empty.setNext(0);
        writeBlock(addr, empty);

        // Pridaj do freeBlocks ak tam ešte nie je
        if (!freeBlocks.contains(addr)) {
            freeBlocks.add(addr);
            Collections.sort(freeBlocks);  // Drž sorted
        }
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

    public void shrinkFileCompletely() throws Exception {
        long fileLen = raf.length();

        if (fileLen == 0) return;

        // Prechádzaj od konca a škrtaj prázdne bloky
        while (fileLen > 0) {
            long lastBlockAddr = fileLen - blockSize;
            if (lastBlockAddr < 0) break;

            Block<T> block = readBlock(lastBlockAddr);

            if (block.getValidCount() == 0) {
                // Prázdny blok - odsekni ho
                fileLen -= blockSize;
            } else {
                // Našli sme neprázdny blok - koniec
                break;
            }
        }

        // Nastav novú dĺžku súboru
        raf.setLength(fileLen);

        // ✅ Vyčisti freeBlocks - vytvor final premennú pre lambdu
        final long newLength = fileLen;
        freeBlocks.removeIf(addr -> addr >= newLength);
    }

    public void writeBlockDirect(long offset, Block<T> b) throws Exception {
        writeBlock(offset, b);
    }
    public int getBlockFactor() {
        return blockFactor;
    }

}