import DataStructures.HeapFile;
import Model.Person;

import java.util.ArrayList;
import java.util.Random;

public class Tester {

    private static final Random rnd = new Random(System.currentTimeMillis());

    public static void runHeapFileTest(HeapFile<Person> hf,
                                       int inserts,
                                       int deletes,
                                       int gets,
                                       int checkInterval) throws Exception {

        ArrayList<Person> persons = new ArrayList<>();
        ArrayList<Long> addrs = new ArrayList<>();

        int opCounter = 0;

        for (int i = 0; i < inserts; i++) {
            Person p = randomPerson();
            long addr = hf.insert(p);

            persons.add(p);
            addrs.add(addr);

            opCounter++;
            if (opCounter % checkInterval == 0) {
                validateHeapFile(hf, persons);
            }
        }

        for (int i = 0; i < deletes; i++) {

            if (persons.isEmpty()) break;

            int index = rnd.nextInt(persons.size());

            Person p = persons.get(index);
            long addr = addrs.get(index);

            hf.delete(addr, p);

            persons.remove(index);
            addrs.remove(index);

            opCounter++;
            if (opCounter % checkInterval == 0) {
                validateHeapFile(hf, persons);
            }
        }

        for (int i = 0; i < gets; i++) {

            if (persons.isEmpty()) break;

            int index = rnd.nextInt(persons.size());

            Person target = persons.get(index);
            long addr = addrs.get(index);

            Person found = hf.get(addr, target);

            if (found == null) {
                throw new RuntimeException("GET ERROR: očakávaný záznam nebol najdený na adrese=" + addr +
                        " id=" + target.getId());
            }

            if (!found.isEqual(target)) {
                throw new RuntimeException("GET ERROR: nesprávný záznam očakávaný=" +
                        target.getId() + " nájdené=" + found.getId());
            }

            opCounter++;
            if (opCounter % checkInterval == 0) {
                validateHeapFile(hf, persons);
            }
        }

        validateHeapFile(hf, persons);
    }


    private static void validateHeapFile(HeapFile<Person> hf,
                                              ArrayList<Person> expected) throws Exception {

        ArrayList<Person> actual = new ArrayList<>();

        long fileLength = hf.getFileLength();
        int blockSize = hf.getBlockSize();

        for (long addr = 0; addr + blockSize <= fileLength; addr += blockSize) {

            var block = hf.readBlockForTest(addr);
            int vc = block.getValidCount();

            for (int i = 0; i < vc; i++) {
                Person p = block.getList().get(i);
                actual.add(p);
            }
        }

        if (actual.size() != expected.size()) {
            throw new RuntimeException(
                    "heapfile error — rozdielny počet záznamov: " +
                            "očakávané=" + expected.size() +
                            ", nájdené=" + actual.size()
            );
        }

        for (Person p : expected) {
            boolean ok = false;
            for (Person a : actual) {
                if (a.isEqual(p)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                throw new RuntimeException(
                        "heapfile error — chýbajúci záznam: " + p.getId()
                );
            }
        }
    }



    private static Person randomPerson() {
        String id = "ID" + rnd.nextInt(10_000_000);
        String name = randomString(6);
        String surname = randomString(8);

        int year = 1980 + rnd.nextInt(30);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);

        return new Person(name, surname, id, year, month, day);
    }

    private static String randomString(int len) {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(letters.charAt(rnd.nextInt(letters.length())));
        }
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }
}
