import java.io.File;
import java.util.ArrayList;
import java.util.Random;

public class Main {

    private static final Random rnd = new Random();

    public static void main(String[] args) throws Exception {

        System.out.println("===== RANDOM HEAPFILE TEST START =====");

        // ZMAŽ STARÉ SÚBORY
        new File("data.bin").delete();
        new File("data.bin.meta").delete();

        HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());

        // ============================================
        // 1) GENERUJ 1000 NÁHODNÝCH OSÔB
        // ============================================
        ArrayList<Person> persons = new ArrayList<>();
        ArrayList<Long> addrs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            Person p = randomPerson(i);
            long addr = hf.insert(p);

            persons.add(p);
            addrs.add(addr);
        }

        System.out.println("Inserted 1000 random persons.");


        // ============================================
        // 2) OTESTUJ 20 NÁHODNÝCH GET
        // ============================================
        for (int i = 0; i < 20; i++) {
            int idx = rnd.nextInt(1000);

            Person expected = persons.get(idx);
            long addr = addrs.get(idx);

            Person found = hf.get(addr, expected);
            if (found == null)
                throw new RuntimeException("GET ERROR: Person not found at index " + idx);

            if (!found.isEqual(expected))
                throw new RuntimeException("GET ERROR: Wrong record retrieved!");
        }

        System.out.println("GET test OK.");


        // ============================================
        // 3) OTESTUJ 20 NÁHODNÝCH DELETE
        // ============================================
        for (int i = 0; i < 20; i++) {
            int idx = rnd.nextInt(1000);

            Person toDelete = persons.get(idx);
            long addr = addrs.get(idx);

            boolean ok = hf.delete(addr, toDelete);
            if (!ok)
                throw new RuntimeException("DELETE ERROR: Failed to delete!");

            // OVER: už NESMIE existovať v bloku
            Block<Person> b = hf.readBlockForTest(addr);

            for (int j = 0; j < b.getValidCount(); j++) {
                Person x = b.getList().get(j);
                if (x.isEqual(toDelete))
                    throw new RuntimeException("DELETE ERROR: Record still exists!");
            }
        }

        System.out.println("DELETE test OK.");


        // ============================================
        // 4) DEBUG PRINT – CELÝ OBSAH
        // ============================================
        System.out.println("\n===== FINAL HEAPFILE CONTENT =====");
        hf.print();

        hf.close();

        System.out.println("\n===== TEST FINISHED SUCCESSFULLY =====");
    }

    // ==========================================================
    // GENERÁTOR NÁHODNEJ OSOBY
    // ==========================================================
    private static Person randomPerson(int idCounter) {
        String[] names = {"Adam","Jozef","Martin","Peter","David","Lukas","Robo","Marek","Tomas","Igor"};
        String[] surnames = {"Novak","Hrasko","Urban","Marek","Janik","Horvat","Kovac","Bodnar","Mikula","Sebo"};

        String name = names[rnd.nextInt(names.length)];
        String surname = surnames[rnd.nextInt(surnames.length)];

        String id = "ID" + idCounter;   // garantuje unikátnosť

        int year = 1980 + rnd.nextInt(30);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);

        return new Person(name, surname, id, year, month, day);
    }
}
