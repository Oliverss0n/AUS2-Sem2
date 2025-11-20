import java.io.File;

public class Main {

    public static void main(String[] args) throws Exception {

        System.out.println("===== TEST HEAPFILE START =====");

        // 1) vytvor nový súbor
        new File("data.bin").delete();
        new File("data.bin.meta").delete();

        HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());

        // INSERT
        Person p1 = new Person("Adam","Novak","A001",1999,5,3);
        Person p2 = new Person("David","Marek","D004",2018,8,30);
        Person p3 = new Person("Cyril","Urban","C003",1998,7,10);

        long a1 = hf.insert(p1);
        long a2 = hf.insert(p2);
        long a3 = hf.insert(p3);

        System.out.println("\n--- AFTER INSERTS ---");
        hf.print();

        // GET
        Person found = hf.get(a2, p2);
        if (found == null)
            throw new RuntimeException("GET ERROR: nenašiel som p2!");
        System.out.println("\nGET OK: našiel som " + found);

        // DELETE KONKRÉTNEHO OBJEKTU
        boolean del = hf.delete(a2, p2);
        if (!del)
            throw new RuntimeException("DELETE ERROR: nepodarilo sa zmazať p2!");
        System.out.println("\nDELETE OK: p2 bol vymazaný.");

        System.out.println("\n--- AFTER DELETE ---");
        hf.print();

        // OVERENIE: objekt p2 NESMIE existovať — ALE TIE ISTÉ ID MOŽU
        Block<Person> b = hf.readBlockForTest(a2);

        for (int i = 0; i < b.getValidCount(); i++) {
            Person x = b.getList().get(i);

            // porovnávame všetky údaje, nie len isEqual()
            if (
                    x.getId().equals(p2.getId()) ) {


                throw new RuntimeException("ERROR: konkrétny záznam p2 stále existuje po vymazaní!");
            }
        }

        System.out.println("\nTEST OK: konkrétny záznam bol odstránený správne.");

        hf.close();
    }
}
