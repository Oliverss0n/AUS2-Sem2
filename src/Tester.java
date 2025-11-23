import DataStructures.Block;
import DataStructures.HeapFile;
import Model.Person;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class Tester {

    private static final Random rnd = new Random();

    public static void main(String[] args) throws Exception {

        System.out.println("===== RANDOM HEAPFILE TEST START =====");

        // ZMAŽ STARÉ SÚBORY
        new File("data.bin").delete();
        new File("data.bin.meta").delete();

        // DataStructure.HeapFile: cesta, blockSize, vzor T
        HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());

        // ================================
        // 1) GENERUJ 100 OSÔB + INSERT
        // ================================
        ArrayList<Person> persons = new ArrayList<>();
        ArrayList<Long> addrs   = new ArrayList<>();
        ArrayList<Person> alive = new ArrayList<>();

        for (int i = 0; i < 100; i++) {

            Person p = randomPerson(i);
            long addr = hf.insert(p);

            persons.add(p);
            addrs.add(addr);
            alive.add(p);
        }

        System.out.println("Inserted 100 random persons.");


        // ================================
        // 2) OTESTUJ 20 NÁHODNÝCH GET
        // ================================
        for (int i = 0; i < 20; i++) {

            int idx = rnd.nextInt(100);   // ← upravené z 1000 na 100

            Person expected = persons.get(idx);
            long   addr     = addrs.get(idx);

            Person found = hf.get(addr, expected);

            if (found == null)
                throw new RuntimeException("GET ERROR: Model.Person not found at index " + idx);

            if (!found.isEqual(expected))
                throw new RuntimeException("GET ERROR: Wrong record retrieved!");
        }

        System.out.println("GET test OK.");

        // Voliteľné: raw bajty súboru
        byte[] b = Files.readAllBytes(Path.of("data.bin"));
        System.out.println("RAW BYTES (preview): " +
                Arrays.toString(Arrays.copyOf(b, Math.min(b.length, 64))));


        // =======================================
        // 3) DELETE — 20 BEZ OPAKOVANIA INDEXOV
        // =======================================
        ArrayList<Integer> indices = new ArrayList<>();
        for (int i = 0; i < 100; i++) indices.add(i);   // ← upravené z 1000
        Collections.shuffle(indices);

        for (int k = 0; k < 20; k++) {

            int idx = indices.get(k);

            Person toDelete = persons.get(idx);
            long addr = addrs.get(idx);

            boolean ok = hf.delete(addr, toDelete);
            if (!ok)
                throw new RuntimeException("DELETE ERROR: delete() returned false for index " + idx);

            alive.remove(toDelete);

            // kontrola v bloku
            Block<Person> b2 = hf.readBlockForTest(addr);
            for (int j = 0; j < b2.getValidCount(); j++) {
                if (b2.getList().get(j).isEqual(toDelete))
                    throw new RuntimeException("DELETE ERROR: record still exists in block!");
            }

            // extra kontrola
            for (Person left : alive) {
                if (left.isEqual(toDelete))
                    throw new RuntimeException("DELETE ERROR: alive list still contains record!");
            }
        }

        System.out.println("DELETE test OK.");

        // ====================================
        // 4) VYPÍŠ CELÝ OBSAH HEAPFILE
        // ====================================
        System.out.println("\n===== FINAL HEAPFILE CONTENT =====");
        hf.print();

        // Zatvor súbory + ulož meta informácie
        hf.close();

        System.out.println("\n===== TEST FINISHED SUCCESSFULLY =====");
    }


    // ====================================================
    // GENERÁTOR NÁHODNEJ OSOBY
    // ====================================================
    private static Person randomPerson(int idCounter) {
        String[] names = {"Adam","Jozef","Martin","Peter","David","Lukas","Robo","Marek","Tomas","Igor"};
        String[] surnames = {"Novak","Hrasko","Urban","Marek","Janik","Horvat","Kovac","Bodnar","Mikula","Sebo"};

        String name = names[rnd.nextInt(names.length)];
        String surname = surnames[rnd.nextInt(surnames.length)];
        String id = "ID" + idCounter;

        int year = 1980 + rnd.nextInt(30);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);

        return new Person(name, surname, id, year, month, day);
    }
}
