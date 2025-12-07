package Model;

import DataStructures.LinearHashFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MainModel {

    private final LinearHashFile<Person> people;
    private final LinearHashFile<PCRTest> tests;

    private final Random rnd = new Random();
    private int testIdCounter = 1;
    private int personIdCounter = 1;

    public MainModel() throws Exception {

        this.people = new LinearHashFile<>(
                "persons_main.bin",
                256,
                "persons_overflow.bin",
                256,
                4,
                new Person()
        );

        this.tests = new LinearHashFile<>(
                "tests_main.bin",
                256,
                "tests_overflow.bin",
                256,
                4,
                new PCRTest()
        );
    }

    // ─────────────────────────────────────────
    // PERSON OPERATIONS
    // ─────────────────────────────────────────

    public boolean insertPerson(Person p) {
        try {
            people.insert(p);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Person findPerson(String id) {
        try {
            Person dummy = new Person();
            dummy.fromId(id);
            return people.find(dummy);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean deletePerson(String id) {
        try {
            Person dummy = new Person();
            dummy.fromId(id);
            return people.delete(dummy);
        } catch (Exception e) {
            return false;
        }
    }

    /*
    public boolean editPerson(String id, Person newData) {
        try {
            Person p = findPerson(id);
            if (p == null) return false;

            p.setName(newData.getName());
            p.setSurname(newData.getSurname());
            p.setYear(newData.getYear());
            p.setMonth(newData.getMonth());
            p.setDay(newData.getDay());

            return people.update(p, p); // pattern == newRecord

        } catch (Exception e) {
            return false;
        }
    }*/

    public boolean editPerson(String id, Person newData) {
        try {
            // 1️⃣ Načítaj starú osobu (má testCodes)
            Person oldPerson = findPerson(id);
            if (oldPerson == null) return false;

            // 2️⃣ Skopíruj testCodes do novej osoby
            for (int testCode : oldPerson.getTestCodes()) {
                newData.addTestCode(testCode);
            }

            // 3️⃣ Update - pattern má len ID
            Person pattern = new Person();
            pattern.fromId(id);

            return people.update(pattern, newData);

        } catch (Exception e) {
            return false;
        }
    }




    public List<Person> generatePersons(int count) {
        List<Person> out = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Person p = randomPerson();
            out.add(p);
            try { people.insert(p); } catch (Exception ignored) {}
        }

        return out;
    }

    // ─────────────────────────────────────────
    // TEST OPERATIONS
    // ─────────────────────────────────────────

    /*
    public boolean editTest(int code, PCRTest newData) {
        try {
            PCRTest t = findTest(code);
            if (t == null) return false;

            t.setPatientId(newData.getPatientId());
            t.setTimestamp(newData.getTimestamp());
            t.setResult(newData.isResult());

            return tests.update(t, t);

        } catch (Exception e) {
            return false;
        }
    }*/
    public boolean editTest(int code, PCRTest newData) {
        try {
            // ✅ Načítaj starý test
            PCRTest oldTest = findTest(code);
            if (oldTest == null) return false;

            // ✅ KRITICKÉ: Zachovaj pôvodný patientId
            newData.setPatientId(oldTest.getPatientId());

            // ✅ Update
            PCRTest pattern = new PCRTest();
            pattern.setTestCode(code);

            return tests.update(pattern, newData);

        } catch (Exception e) {
            return false;
        }
    }



    public PCRTest findTest(int testCode) {
        try {
            PCRTest dummy = new PCRTest();
            dummy.setTestCode(testCode);
            return tests.find(dummy);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean deleteTest(int testCode) {
        try {
            PCRTest dummy = new PCRTest();
            dummy.setTestCode(testCode);
            return tests.delete(dummy);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean insertTest(PCRTest t) {
        try {
            tests.insert(t);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ─────────────────────────────────────────
    // PRINTING
    // ─────────────────────────────────────────

    public String printPeople() {
        try {
            return people.print();
        } catch (Exception e) {
            return "[PRINT ERROR] " + e.getMessage();
        }
    }

    public String printTests() {
        try {
            return tests.print();
        } catch (Exception e) {
            return "[PRINT ERROR] " + e.getMessage();
        }
    }

    // ─────────────────────────────────────────
    // GENERATION HELPERS
    // ─────────────────────────────────────────

    public Person randomPerson() {
        String id = "P" + (personIdCounter++);
        return new Person(
                randomString(6),
                randomString(8),
                id,
                1980 + rnd.nextInt(30),
                1 + rnd.nextInt(12),
                1 + rnd.nextInt(28)
        );
    }



    public Person getRandomPersonSimple() {
        try {
            String dump = people.print(); // print už máš
            String[] lines = dump.split("\n");

            List<Person> persons = new ArrayList<>();

            for (String l : lines) {
                l = l.trim();
                if (l.isEmpty()) continue;
                // Očakávame formát z tvojej print metódy:
                // Meno Priezvisko ID (YYYY-MM-DD)
                String[] parts = l.split(" ");
                if (parts.length < 3) continue;

                String name = parts[0];
                String surname = parts[1];
                String id = parts[2];
                persons.add(new Person(name, surname, id, 0,0,0));
            }

            if (persons.isEmpty()) return null;
            return persons.get(rnd.nextInt(persons.size()));

        } catch (Exception e) {
            return null;
        }
    }

    public ArrayList<PCRTest> getPersonTests(String personId) {
        ArrayList<PCRTest> result = new ArrayList<>();

        Person p = findPerson(personId);
        if (p == null) return result;

        // Person má zoznam testCodeov
        ArrayList<Integer> testCodes = p.getTestCodes();

        for (int code : testCodes) {
            PCRTest test = findTest(code);
            if (test != null) {
                result.add(test);
            }
        }

        return result;
    }



    private String randomString(int len) {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(letters.charAt(rnd.nextInt(letters.length())));
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }

    // ─────────────────────────────────────────
    // CLOSE
    // ─────────────────────────────────────────
    public void close() {
        try {
            people.close();
            tests.close();
        } catch (Exception ignored) {}
    }

    public void generateAllData(int personCount, int testCount, long seed) {
        Random rnd = new Random(seed);
        long start = System.currentTimeMillis();

        System.out.println("=== ŠTART GENEROVANIA ===");

        // 1️⃣ GENERUJ OSOBY
        List<Person> persons = new ArrayList<>();

        for (int i = 0; i < personCount; i++) {
            Person p = new Person(
                    randomString(5 + rnd.nextInt(4)),
                    randomString(6 + rnd.nextInt(5)),
                    "P" + (personIdCounter++),
                    1950 + rnd.nextInt(60),
                    1 + rnd.nextInt(12),
                    1 + rnd.nextInt(28)
            );

            if (insertPerson(p)) {
                persons.add(p);
            }
        }

        if (persons.isEmpty()) {
            System.out.println("⚠️ Žiadne osoby neboli vložené!");
            return;
        }

        System.out.println("✅ Vložené osoby: " + persons.size());

        // 2️⃣ GENERUJ TESTY
        int successfulTests = 0;

        for (int i = 0; i < testCount; i++) {
            Person patient = persons.get(rnd.nextInt(persons.size()));

            // ✅ NÁHODNÝ DÁTUM A ČAS
            int year = 2020 + rnd.nextInt(5);    // 2020-2024
            int month = 1 + rnd.nextInt(12);     // 1-12
            int day = 1 + rnd.nextInt(28);       // 1-28 (bezpečné pre všetky mesiace)
            int hour = rnd.nextInt(24);          // 0-23
            int minute = rnd.nextInt(60);        // 0-59

            PCRTest t = new PCRTest(
                    testIdCounter++,
                    patient.getId(),
                    year, month, day, hour, minute,  // ✅ NOVÉ - rozdelené polia
                    rnd.nextBoolean(),
                    1 + rnd.nextInt(100),
                    randomString(1 + rnd.nextInt(10))
            );

            // ✅ Vlož test
            if (insertTest(t)) {
                // ✅ Pridaj testCode do Person + ulož do súboru
                if (addTestToPerson(patient.getId(), t.getTestCode())) {
                    successfulTests++;
                }
            }
        }

        long end = System.currentTimeMillis();

        System.out.println(
                "=== GENERÁCIA DOKONČENÁ ===\n" +
                        "Osôb: " + persons.size() + "\n" +
                        "Testov: " + successfulTests + " / " + testCount + "\n" +
                        "Čas: " + (end - start) + " ms\n"
        );
    }

/*
    public void generateAllData(int personCount, int testCount, long seed) {
        Random rnd = new Random(seed);
        long start = System.currentTimeMillis();

        System.out.println("=== ŠTART GENEROVANIA ===");

        // 1️⃣ GENERUJ OSOBY
        List<Person> persons = new ArrayList<>();

        for (int i = 0; i < personCount; i++) {
            Person p = new Person(
                    randomString(5 + rnd.nextInt(4)),
                    randomString(6 + rnd.nextInt(5)),
                    "P" + (personIdCounter++),
                    1950 + rnd.nextInt(60),
                    1 + rnd.nextInt(12),
                    1 + rnd.nextInt(28)
            );

            if (insertPerson(p)) {
                persons.add(p);
            }
        }

        if (persons.isEmpty()) {
            System.out.println("⚠️ Žiadne osoby neboli vložené!");
            return;
        }

        System.out.println("✅ Vložené osoby: " + persons.size());

        // 2️⃣ GENERUJ TESTY
        int successfulTests = 0;

        for (int i = 0; i < testCount; i++) {
            Person patient = persons.get(rnd.nextInt(persons.size()));

            PCRTest t = new PCRTest(
                    testIdCounter++,
                    patient.getId(),
                    System.currentTimeMillis() - rnd.nextInt(1_000_000_000),
                    rnd.nextBoolean(),
                    1 + rnd.nextInt(100),
                    randomString(1 + rnd.nextInt(10))
            );

            // ✅ Vlož test
            if (insertTest(t)) {
                // ✅ Pridaj testCode do Person + ulož do súboru
                if (addTestToPerson(patient.getId(), t.getTestCode())) {
                    successfulTests++;
                }
            }
        }

        long end = System.currentTimeMillis();

        System.out.println(
                "=== GENERÁCIA DOKONČENÁ ===\n" +
                        "Osôb: " + persons.size() + "\n" +
                        "Testov: " + successfulTests + " / " + testCount + "\n" +
                        "Čas: " + (end - start) + " ms\n"
        );
    } */

    /*
    public boolean addTestToPerson(String personId, int testCode) {
        try {
            Person p = findPerson(personId);
            if (p == null) return false;

            if (!p.addTestCode(testCode)) {
                return false;  // Už má max testov
            }

            // ✅ Update v súbore
            Person old = new Person();
            old.fromId(personId);
            people.update(old, p);

            return true;

        } catch (Exception e) {
            return false;
        }
    }*/
    public boolean addTestToPerson(String personId, int testCode) {
        try {
            Person p = findPerson(personId);
            if (p == null) return false;

            if (!p.addTestCode(testCode)) {
                return false;
            }

            Person old = new Person();
            old.fromId(personId);
            people.update(old, p);

            return true;

        } catch (Exception e) {
            return false;
        }
    }

    public int getNextTestId() {
        return testIdCounter++;
    }

}
