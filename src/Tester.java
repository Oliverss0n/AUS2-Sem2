import DataStructures.HeapFile;
import Model.Person;

import java.util.Random;

public class Tester {

    private static final Random rnd = new Random();

    public static void runHeapFileTest(HeapFile<Person> hf, int inserts, int deletes, int gets) throws Exception {

        Person[] inserted = new Person[inserts];
        long[] addrs = new long[inserts];
        int insertIndex = 0;

        for (int i = 0; i < inserts; i++) {
            Person p = randomPerson();
            long addr = hf.insert(p);

            inserted[insertIndex] = p;
            addrs[insertIndex] = addr;
            insertIndex++;
        }

        for (int i = 0; i < deletes; i++) {
            int idx = rnd.nextInt(insertIndex);
            Person p = inserted[idx];

            if (p != null) {
                hf.delete(addrs[idx], p);
                inserted[idx] = null;
            }
        }

        for (int i = 0; i < gets; i++) {
            int idx = rnd.nextInt(insertIndex);
            Person p = inserted[idx];

            if (p != null) {
                hf.get(addrs[idx], p);
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
