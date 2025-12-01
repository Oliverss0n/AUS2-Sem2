import DataStructures.Block;
import DataStructures.HeapFile;
import DataStructures.LinearHashFile;
import Model.Person;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class Tester {

    private static final long SEED =System.currentTimeMillis();
    private static final Random rnd = new Random(SEED);

    public static void mixedHeapTest(HeapFile<Person> hf,
                                     int operations,
                                     int insertPercent,
                                     int deletePercent,
                                     int getPercent,
                                     int checkInterval) throws Exception {

        ArrayList<Person> model = new ArrayList<>();
        ArrayList<Long> addrs = new ArrayList<>();

        for (int op = 1; op <= operations; op++) {

            int choice = rnd.nextInt(100);

            if (choice < insertPercent) {
                Person p = randomPerson();
                long addr = hf.insert(p);
                model.add(p);
                addrs.add(addr);
            }
            else if (choice < insertPercent + deletePercent) {
                if (!model.isEmpty()) {
                    int idx = rnd.nextInt(model.size());
                    Person p = model.get(idx);
                    long addr = addrs.get(idx);
                    hf.delete(addr, p);
                    model.remove(idx);
                    addrs.remove(idx);
                }
            }
            else {
                if (!model.isEmpty()) {
                    int idx = rnd.nextInt(model.size());
                    Person expected = model.get(idx);
                    long addr = addrs.get(idx);

                    Person found = hf.get(addr, expected);
                    if (found == null || !found.isEqual(expected))
                        throw new RuntimeException("GET ERROR: expected=" + expected.getId());
                }
            }

            if (op % checkInterval == 0) {
                validateWholeHeapFile(hf, model);
            }
        }

        validateWholeHeapFile(hf, model);
    }


    private static void validateWholeHeapFile(HeapFile<Person> hf,
                                              ArrayList<Person> expected) throws Exception {

        ArrayList<Person> actual = new ArrayList<>();

        long fileLength = hf.getFileLength();
        int blockSize = hf.getBlockSize();

        for (long addr = 0; addr + blockSize <= fileLength; addr += blockSize) {
            var block = hf.readBlock(addr);
            for (int i = 0; i < block.getValidCount(); i++)
                actual.add(block.getList().get(i));
        }

        if (actual.size() != expected.size())
            throw new RuntimeException("COUNT ERROR: expected=" + expected.size() + " actual=" + actual.size());

        for (Person p : expected) {
            boolean ok = false;
            for (Person a : actual) {
                if (a.isEqual(p)) {
                    ok = true;
                    break;
                }
            }
            if (!ok)
                throw new RuntimeException("MISSING RECORD: " + p.getId());
        }
    }



    public static void mixedLinearHashTest(LinearHashFile<Person> lhf,
                                           int operations,
                                           int insertPercent,
                                           int deletePercent,
                                           int findPercent,
                                           int checkInterval) throws Exception {

        ArrayList<Person> model = new ArrayList<>();

        for (int op = 1; op <= operations; op++) {

            int choice = rnd.nextInt(100);

            // ========================================
            // INSERT
            // ========================================
            if (choice < insertPercent) {

                Person p = randomPerson();
                lhf.insert(p);
                model.add(p);
            }

            // ========================================
            // DELETE
            // ========================================
            else if (choice < insertPercent + deletePercent) {

                if (!model.isEmpty()) {
                    int idx = rnd.nextInt(model.size());
                    Person p = model.get(idx);

                    //lhf.delete(p);       // delete by pattern
                    model.remove(idx);   // remove from reference model
                }
            }

            // ========================================
            // FIND
            // ========================================
            else {

                if (!model.isEmpty()) {
                    int idx = rnd.nextInt(model.size());
                    Person expected = model.get(idx);

                    Person found = lhf.find(expected);

                    if (found == null || !found.isEqual(expected)) {
                        throw new RuntimeException("FIND ERROR: expected=" + expected.getId());
                    }
                }
            }

            // ========================================
            // VALIDATE PERIODICALLY
            // ========================================
            if (op % checkInterval == 0) {
                validateWholeLinearHash(lhf, model);
            }
        }

        // final validation
        validateWholeLinearHash(lhf, model);
    }

    private static void validateWholeLinearHash(LinearHashFile<Person> lhf,
                                                ArrayList<Person> expected) throws Exception {

        ArrayList<Person> actual = new ArrayList<>();

        int groups = lhf.getS() + lhf.getM() * (int)Math.pow(2, lhf.getU());
        int blockSize = lhf.getMainFile().getBlockSize();

        for (int i = 0; i < groups; i++) {

            long addr = (long) i * blockSize;
            Block<Person> block = lhf.getMainFile().readBlock(addr);

            for (int j = 0; j < block.getValidCount(); j++) {
                actual.add(block.getList().get(j));
            }

            long next = block.getNext();
            while (next != -1) {  // ✅ OPRAVENÉ
                Block<Person> ov = lhf.getOverflowFile().readBlock(next);
                for (int j = 0; j < ov.getValidCount(); j++) {
                    actual.add(ov.getList().get(j));
                }
                next = ov.getNext();
            }
        }

        if (actual.size() != expected.size())
            throw new RuntimeException("COUNT ERROR: expected=" + expected.size() + " actual=" + actual.size());

        for (Person p : expected) {
            boolean ok = false;
            for (Person a : actual) {
                if (a.isEqual(p)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                throw new RuntimeException("MISSING RECORD: " + p.getId());
            }
        }
    }

    private static int computeIndexManual(int key, int M, int S, int u) {
        int pow2 = (int)Math.pow(2, u);
        int range = M * pow2;

        int i = key % range;
        if (i < 0) i += range;

        if (i < S) {
            int extRange = M * (pow2 * 2);
            i = key % extRange;
            if (i < 0) i += extRange;
        }

        return i;
    }

    public static void ultimateLinearHashValidation(LinearHashFile<Person> lhf,
                                                    ArrayList<Person> model) throws Exception {

        System.out.println("\n=== ULTIMATE LH VALIDATION START ===");

        int M = lhf.getM();
        int u = lhf.getU();
        int S = lhf.getS();

        int groups = S + M * (int)Math.pow(2, u);
        int blockSize = lhf.getMainFile().getBlockSize();

        // Zozbierame realitu
        ArrayList<Person> actual = new ArrayList<>();

        // mapovanie bucket → záznamy
        ArrayList<ArrayList<Person>> bucketMap = new ArrayList<>();
        for (int i = 0; i < groups; i++) {
            bucketMap.add(new ArrayList<>());
        }

        // ==============================================
        // 1. Prejdi všetky primárne bloky a overflow
        // ==============================================
        for (int i = 0; i < groups; i++) {

            long addr = (long) i * blockSize;
            Block<Person> block = lhf.getMainFile().readBlock(addr);

            // primárne záznamy
            for (int j = 0; j < block.getValidCount(); j++) {
                Person p = block.getList().get(j);
                actual.add(p);
                bucketMap.get(i).add(p);
            }

            // overflow reťazec
            long next = block.getNext();
            HashSet<Long> visited = new HashSet<>();  // detekcia cyklu

            while (next != -1) {

                if (visited.contains(next))
                    throw new RuntimeException("CYCLE DETECTED in overflow chain for bucket " + i);

                visited.add(next);

                Block<Person> ov = lhf.getOverflowFile().readBlock(next);
                for (int j = 0; j < ov.getValidCount(); j++) {
                    Person p = ov.getList().get(j);
                    actual.add(p);
                    bucketMap.get(i).add(p);
                }

                next = ov.getNext();
            }
        }


        // ==============================================
        // 2. Skontroluj počet záznamov
        // ==============================================
        if (model.size() != actual.size()) {
            throw new RuntimeException("COUNT MISMATCH: expected=" + model.size() +
                    " actual=" + actual.size());
        }


        // ==============================================
        // 3. Skontroluj, že každý záznam je v správnom buckete
        // ==============================================
        for (Person p : model) {
            int key = p.getHashCode();
            int idx = computeIndexManual(key, M, S, u); // manuálne vypočítame

            boolean ok = false;
            for (Person stored : bucketMap.get(idx)) {
                if (stored.isEqual(p)) {
                    ok = true;
                    break;
                }
            }

            if (!ok) {
                throw new RuntimeException(
                        "WRONG BUCKET: record " + p.getId() +
                                " SHOULD BE IN " + idx +
                                " BUT IS NOT THERE!"
                );
            }
        }


        // ==============================================
        // 4. Kontrola súvislosti overflow reťazcov
        // ==============================================
        for (int i = 0; i < groups; i++) {

            long addr = (long) i * blockSize;
            Block<Person> blk = lhf.getMainFile().readBlock(addr);

            long next = blk.getNext();

            if (next == 0) {
                throw new RuntimeException("ERROR: primary block " + i +
                        " has next=0 (must be -1 or valid addr)");
            }

            // všetky visited overflow na overenie sirot
            HashSet<Long> seen = new HashSet<>();

            while (next != -1) {
                if (seen.contains(next))
                    throw new RuntimeException("CYCLE in overflow chain for bucket " + i);

                seen.add(next);

                Block<Person> ov = lhf.getOverflowFile().readBlock(next);
                if (ov.getValidCount() == 0) {
                    throw new RuntimeException("EMPTY BLOCK found inside overflow chain of bucket " + i);
                }

                next = ov.getNext();
                if (next == 0)
                    throw new RuntimeException("ERROR: overflow block has next=0 (invalid pointer)");
            }
        }

        System.out.println("ULTIMATE VALIDATION PASSED ✓✓✓");
        System.out.println("=== ULTIMATE LH VALIDATION END ===\n");
    }



    public static void simpleInsertFindTest(LinearHashFile<Person> lhf,
                                            int inserts,
                                            int checks) throws Exception {


        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║  TEST STARTED WITH SEED: " + SEED);
        System.out.println("║  To reproduce: change SEED in Tester.java");
        System.out.println("╚════════════════════════════════════════╝\n");

        ArrayList<Person> model = new ArrayList<>();

        // ==========================================
        // INSERT TEST s periodickou validáciou
        // ==========================================
        for (int i = 0; i < inserts; i++) {
            Person p = randomPerson();
            lhf.insert(p);
            model.add(p);

            // ✅ Validuj každých 100 insertov
            if ((i + 1) % 10 == 0) {
                System.out.println("Validating after " + (i + 1) + " inserts...");
                ultimateLinearHashValidation(lhf, model);
            }
        }

        System.out.println("INSERT DONE, inserted = " + inserts);
        System.out.println(lhf.print());

        // ==========================================
        // FIND TEST
        // ==========================================
        for (int i = 0; i < checks; i++) {
            Person expected = model.get(rnd.nextInt(model.size()));
            Person found = lhf.find(expected);

            if (found == null || !found.isEqual(expected)) {
                throw new RuntimeException(
                        "FIND ERROR: expected = " + expected.getId() +
                                " found = " + (found != null ? found.getId() : "null")
                );
            }
        }

        System.out.println("FIND OK (" + checks + " random finds)");

        // ✅ Finálna validácia
        ultimateLinearHashValidation(lhf, model);
    }



    private static Person randomPerson() {
        String id = "ID" + rnd.nextInt(10_000_000);
        return new Person(
                randomString(6),
                randomString(8),
                id,
                1980 + rnd.nextInt(30),
                1 + rnd.nextInt(12),
                1 + rnd.nextInt(28)
        );
    }

    private static String randomString(int len) {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(letters.charAt(rnd.nextInt(letters.length())));
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }
}
