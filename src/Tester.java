import DataStructures.HeapFile;
import Model.Person;

import java.util.ArrayList;
import java.util.Random;

public class Tester {

    private static final Random rnd = new Random(System.currentTimeMillis());

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
            var block = hf.readBlockForTest(addr);
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
