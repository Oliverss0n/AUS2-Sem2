package Model;

import DataStructures.LinearHashFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MainModel {

    private LinearHashFile<Person> people;
    private LinearHashFile<PCRTest> tests;

    private boolean isDbOpen = false;

    private final Random rnd = new Random();
    private int testIdCounter = 1;
    private int personIdCounter = 1;

    public MainModel() throws Exception {

    }


    public void openDatabase(String peoplePath, String testsPath) throws Exception {
        if (isDbOpen) {
            closeDatabase();
        }

        this.people = new LinearHashFile<>(
                peoplePath + "_main.bin",
                256,
                peoplePath + "_overflow.bin",
                256,
                4,
                new Person()
        );

        this.tests = new LinearHashFile<>(
                testsPath + "_main.bin",
                256,
                testsPath + "_overflow.bin",
                256,
                4,
                new PCRTest()
        );

        people.enableDensitySplit(1.2);
        people.enableBucketSizeSplit(6);
        people.enableOverflowCountSplit(2);

        tests.enableDensitySplit(1.2);
        tests.enableBucketSizeSplit(6);
        tests.enableOverflowCountSplit(2);

        isDbOpen = true;
    }


    public void createDatabase(String peoplePath, String testsPath,
                               int mainBlockSize, int overflowBlockSize,
                               int M) throws Exception {
        if (isDbOpen) {
            closeDatabase();
        }

        this.people = new LinearHashFile<>(
                peoplePath + "_main.bin",
                mainBlockSize,
                peoplePath + "_overflow.bin",
                overflowBlockSize,
                M,
                new Person()
        );

        this.tests = new LinearHashFile<>(
                testsPath + "_main.bin",
                mainBlockSize,
                testsPath + "_overflow.bin",
                overflowBlockSize,
                M,
                new PCRTest()
        );

        people.enableDensitySplit(1.2);
        people.enableBucketSizeSplit(6);
        people.enableOverflowCountSplit(2);

        tests.enableDensitySplit(1.2);
        tests.enableBucketSizeSplit(6);
        tests.enableOverflowCountSplit(2);

        isDbOpen = true;
    }

    public void closeDatabase() throws Exception {
        if (people != null) {
            people.close();
            people = null;
        }
        if (tests != null) {
            tests.close();
            tests = null;
        }
        isDbOpen = false;
    }


    public boolean isDbOpen() {
        return isDbOpen;
    }


    private void checkDbOpen() {
        if (!isDbOpen) {
            throw new IllegalStateException("Database is not open! Use openDatabase() or createDatabase() first.");
        }
    }


    public boolean insertPerson(Person p) {
        try {
            checkDbOpen();
            people.insert(p);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Person findPerson(String id) {
        try {
            checkDbOpen();
            Person dummy = new Person();
            dummy.fromId(id);
            return people.find(dummy);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean deletePerson(String id) {
        try {
            checkDbOpen();
            Person dummy = new Person();
            dummy.fromId(id);
            return people.delete(dummy);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean editPerson(String id, Person newData) {
        try {
            checkDbOpen();
            Person old = findPerson(id);
            if (old == null) return false;

            Person updated = new Person(
                    newData.getName(),
                    newData.getSurname(),
                    id,
                    newData.getYear(),
                    newData.getMonth(),
                    newData.getDay()
            );

            for (int code : old.getTestCodes()) {
                updated.addTestCode(code);
            }

            Person pattern = new Person();
            pattern.fromId(id);

            return people.update(pattern, updated);

        } catch (Exception e) {
            return false;
        }
    }


    public boolean insertTest(PCRTest t) {
        try {
            checkDbOpen();
            tests.insert(t);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public PCRTest findTest(int testCode) {
        try {
            checkDbOpen();
            PCRTest dummy = new PCRTest();
            dummy.setTestCode(testCode);
            return tests.find(dummy);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean deleteTest(int testCode) {
        try {
            checkDbOpen();
            PCRTest dummy = new PCRTest();
            dummy.setTestCode(testCode);
            return tests.delete(dummy);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean editTest(int code, PCRTest newData) {
        try {
            checkDbOpen();
            PCRTest old = findTest(code);
            if (old == null) return false;

            PCRTest updated = new PCRTest(
                    code,
                    old.getPatientId(),
                    newData.getTimestamp(),
                    newData.isResult(),
                    newData.getValue(),
                    newData.getNote()
            );

            PCRTest pattern = new PCRTest();
            pattern.setTestCode(code);

            return tests.update(pattern, updated);

        } catch (Exception e) {
            return false;
        }
    }


    public boolean addTestToPerson(String personId, int testCode) {
        try {
            checkDbOpen();
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

    public ArrayList<PCRTest> getPersonTests(String personId) {
        ArrayList<PCRTest> result = new ArrayList<>();

        try {
            checkDbOpen();
            Person p = findPerson(personId);
            if (p == null) return result;

            ArrayList<Integer> testCodes = p.getTestCodes();

            for (int code : testCodes) {
                PCRTest test = findTest(code);
                if (test != null) {
                    result.add(test);
                }
            }
        } catch (Exception e) {
        }

        return result;
    }

    public String printPeople() {
        try {
            checkDbOpen();
            return people.print();
        } catch (Exception e) {
            return "[PRINT ERROR] " + e.getMessage();
        }
    }

    public String printTests() {
        try {
            checkDbOpen();
            return tests.print();
        } catch (Exception e) {
            return "PRINT ERROR " + e.getMessage();
        }
    }

    public void generateAllData(int personCount, int testCount, long seed) {
        try {
            checkDbOpen();
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            return;
        }

        Random rnd = new Random(seed);

        System.out.println("=== ŠTART GENEROVANIA ===");

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
            System.out.println("Žiadne osoby neboli vložené!");
            return;
        }

        System.out.println("Vložené osoby: " + persons.size());

        int successfulTests = 0;

        for (int i = 0; i < testCount; i++) {
            Person patient = persons.get(rnd.nextInt(persons.size()));

            int year = 2020 + rnd.nextInt(5);
            int month = 1 + rnd.nextInt(12);
            int day = 1 + rnd.nextInt(28);
            int hour = rnd.nextInt(24);
            int minute = rnd.nextInt(60);

            long timestamp = PCRTest.makeTimestamp(year, month, day, hour, minute);

            PCRTest t = new PCRTest(
                    testIdCounter++,
                    patient.getId(),
                    timestamp,
                    rnd.nextBoolean(),
                    1 + rnd.nextInt(100),
                    randomString(1 + rnd.nextInt(10))
            );

            if (insertTest(t)) {
                if (addTestToPerson(patient.getId(), t.getTestCode())) {
                    successfulTests++;
                }
            }
        }

        System.out.println(
                "=== GENEROVANIE DOKONČENÉ ===\n" +
                        "Osôb: " + persons.size() + "\n" +
                        "Testov: " + successfulTests + " / " + testCount + "\n"
        );
    }

    private String randomString(int len) {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(letters.charAt(rnd.nextInt(letters.length())));
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }

    public void close() {
        try {
            if (isDbOpen) {
                closeDatabase();
            }
        } catch (Exception e) {
        }
    }
}