import java.io.*;
import java.util.*;

public class HeapFile<T extends IRecord<T>> {

    private RandomAccessFile raf;
    private String path;
    private String metaPath;

    private int blockSize;
    private int blockFactor;

    private ArrayList<Long> freeBlocks; // toto asi Block
    private ArrayList<Long> partialBlocks; // toto asi block

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
    // VYTVORÍ PRÁZDNY BLOK (bez generovania T !)
    // ============================================================
    private Block<T> emptyBlock() {
        ArrayList<T> list = new ArrayList<>(blockFactor);
        for (int i = 0; i < blockFactor; i++) {
            try {
                list.add((T) prototype.getClass().newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Trieda musí mať prázdny konštruktor!");
            }   // TOTO JE JEDINÉ, ČO MUSÍ T MAŤ NAVYŠE
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

            Block<T> b = readBlock(addr);

            int vc = b.getValidCount();
            b.getList().set(vc, data);
            b.setValidCount(vc + 1);

            if (b.getValidCount() == blockFactor)
                partialBlocks.remove(0);

            writeBlock(addr, b);
            return addr;
        }

        // 2) free blok existuje
        if (!freeBlocks.isEmpty()) {
            addr = freeBlocks.remove(0);

            Block<T> b = emptyBlock();
            b.getList().set(0, data);
            b.setValidCount(1);

            if (blockFactor > 1)
                partialBlocks.add(addr);

            writeBlock(addr, b);
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
        Block<T> b = readBlock(addr);

        for (int i = 0; i < b.getValidCount(); i++) {
            T x = b.getList().get(i);
            if (x.isEqual(pattern)) return x;
        }
        return null;
    }

    // ============================================================
    // DELETE
    // ============================================================
    public boolean delete(long addr, T pattern) throws Exception {
        Block<T> b = readBlock(addr);
        int vc = b.getValidCount();

        int index = -1;
        for (int i = 0; i < vc; i++) {
            if (b.getList().get(i).isEqual(pattern)) {
                index = i;
                break;
            }
        }

        if (index == -1) return false;

        b.getList().set(index, b.getList().get(vc - 1));
        b.setValidCount(vc - 1);

        // blok sa vyprázdnil
        if (b.getValidCount() == 0)
            handleEmptyBlock(addr);

        else if (!partialBlocks.contains(addr))
            partialBlocks.add(addr);

        writeBlock(addr, b);
        return true;
    }

    private void handleEmptyBlock(long addr) throws Exception {
        long fileEnd = raf.length() - blockSize;

        if (addr == fileEnd)
            shrinkFile();
        else
            freeBlocks.add(addr);
    }

    // ============================================================
    // SKRÁTENIE SÚBORU – podľa tvojich poznámok
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
        byte[] raw = new byte[arr.size()];

        for (int i = 0; i < raw.length; i++) raw[i] = arr.get(i);

        raf.seek(addr);
        raf.write(raw);
    }

    // ============================================================
    // PRINT
    // ============================================================
    public void print() throws Exception {
        long size = raf.length();
        for (long addr = 0; addr < size; addr += blockSize) {
            Block<T> b = readBlock(addr);

            System.out.println("Block @" + addr);
            System.out.println("Valid: " + b.getValidCount());
            for (int i = 0; i < b.getValidCount(); i++)
                System.out.println("   " + b.getList().get(i));
        }
    }

    // ============================================================
    // META – PRESNE TO, ČO ŽIADA UČITEĽ
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
        File f = new File(metaPath);
        if (!f.exists()) return;

        Scanner sc = new Scanner(f);

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
    // CLOSE — ULOŽÍ METADÁTA!
    // ============================================================
    public void close() throws Exception {
        saveMetadata();
        raf.close();
    }


    // DEBUG: aby sme vedeli čítať blok zvonku
    public Block<T> readBlockForTest(long addr) throws Exception {
        return readBlock(addr);
    }

}
