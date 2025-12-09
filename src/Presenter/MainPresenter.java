package Presenter;

import Model.MainModel;
import Model.Person;
import Model.PCRTest;
import View.IMainView;
import View.MainView;

import java.util.ArrayList;

public class MainPresenter {

    private final MainView view;
    private final MainModel model;

    public MainPresenter(MainView view, MainModel model) {
        this.view = view;
        this.model = model;
        view.setPresenter(this);

        updateDbStatus();
    }

    public void onOpenDatabase() {
        try {
            String peoplePath = view.promptInput("Zadaj cestu k people súborom (bez prípony, napr. 'db1/people'):");
            if (peoplePath == null || peoplePath.isEmpty()) return;

            String testsPath = view.promptInput("Zadaj cestu k tests súborom (bez prípony, napr. 'db1/tests'):");
            if (testsPath == null || testsPath.isEmpty()) return;

            model.openDatabase(peoplePath, testsPath);

            view.showMessage("Databáza úspešne otvorená!\nPeople: " + peoplePath + "\nTests: " + testsPath);
            view.appendOutput("=== DATABASE OPENED ===\n");
            view.appendOutput("People path: " + peoplePath + "\n");
            view.appendOutput("Tests path: " + testsPath + "\n");

            updateDbStatus();

        } catch (Exception e) {
            view.showMessage("Chyba pri otváraní databázy:\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void onCreateDatabase() {
        try {
            String peoplePath = view.promptInput("Zadaj cestu k people súborom (bez prípony, napr. 'db1/people'):");
            if (peoplePath == null || peoplePath.isEmpty()) return;

            String testsPath = view.promptInput("Zadaj cestu k tests súborom (bez prípony, napr. 'db1/tests'):");
            if (testsPath == null || testsPath.isEmpty()) return;

            String blockSizeStr = view.promptInput("Zadaj veľkosť bloku (default 256):");
            int blockSize = 256;
            if (blockSizeStr != null && !blockSizeStr.isEmpty()) {
                try {
                    blockSize = Integer.parseInt(blockSizeStr);
                    if (blockSize < 64 || blockSize > 4096) {
                        view.showMessage("Veľkosť bloku musí byť medzi 64-4096. Použije sa default 256.");
                        blockSize = 256;
                    }
                } catch (NumberFormatException e) {
                    view.showMessage("Neplatné číslo. Použije sa default 256.");
                }
            }

            String mStr = view.promptInput("Zadaj počiatočný M (počet primárnych blokov, default 4):");
            int m = 4;
            if (mStr != null && !mStr.isEmpty()) {
                try {
                    m = Integer.parseInt(mStr);
                    if (m < 2 || m > 16) {
                        view.showMessage("M musí byť medzi 2-16. Použije sa default 4.");
                        m = 4;
                    }
                } catch (NumberFormatException e) {
                    view.showMessage("Neplatné číslo. Použije sa default 4.");
                }
            }

            model.createDatabase(peoplePath, testsPath, blockSize, blockSize, m);

            view.showMessage("Databáza úspešne vytvorená!\n" +
                    "People: " + peoplePath + "\n" +
                    "Tests: " + testsPath + "\n" +
                    "Block size: " + blockSize + "\n" +
                    "M: " + m);

            view.appendOutput("=== NEW DATABASE CREATED ===\n");
            view.appendOutput("People path: " + peoplePath + "\n");
            view.appendOutput("Tests path: " + testsPath + "\n");
            view.appendOutput("Block size: " + blockSize + "\n");
            view.appendOutput("Initial M: " + m + "\n");

            updateDbStatus();

        } catch (Exception e) {
            view.showMessage("Chyba pri vytváraní databázy:\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    public void onCloseDatabase() {
        try {
            if (!model.isDbOpen()) {
                view.showMessage("Žiadna databáza nie je otvorená.");
                return;
            }

            model.closeDatabase();

            view.showMessage("Databáza úspešne zatvorená!");
            view.appendOutput("=== DATABASE CLOSED ===\n");

            updateDbStatus();

        } catch (Exception e) {
            view.showMessage("Chyba pri zatváraní databázy:\n" + e.getMessage());
            e.printStackTrace();
        }
    }


    private void updateDbStatus() {
        if (model.isDbOpen()) {
            view.setDbStatus("Database OPEN", true);
        } else {
            view.setDbStatus("No database open", false);
        }
    }

    public void onGenerateData() {
        try {
            if (!model.isDbOpen()) {
                view.showMessage("Najprv otvor alebo vytvor databázu!");
                return;
            }

            int persons = 1000;
            int tests = 1500;
            long seed = System.currentTimeMillis();

            view.appendOutput(
                    "Generujem dáta...\n" +
                            "- Osoby: " + persons + "\n" +
                            "- Testy: " + tests + "\n"
            );

            model.generateAllData(persons, tests, seed);

            view.showMessage("Dáta úspešne vygenerované!");

        } catch (Exception e) {
            view.showMessage("Chyba pri generovaní dát: " + e.getMessage());
        }
    }

    public void opClose() {
        try {
            if (model.isDbOpen()) {
                model.closeDatabase();
            }
        } catch (Exception e) {
        }
        view.appendOutput("Program ukončený.");
        System.exit(0);
    }

    public void onTaskSelected(int taskId) {
        if (!model.isDbOpen()) {
            view.showMessage("Najprv otvor alebo vytvor databázu!");
            return;
        }

        switch (taskId) {
            case 1 -> opInsertTest();
            case 2 -> opFindPerson();
            case 3 -> opFindTest();
            case 4 -> opInsertPerson();
            case 5 -> opDeleteTest();
            case 6 -> opDeletePerson();
            case 7 -> opEditPerson();
            case 8 -> opEditTest();
            default -> view.showMessage("Neznáma operácia.");
        }
    }

    private void opInsertTest() {
        try {
            String testCodeStr = view.promptInput("Zadaj ID testu (číslo):");
            if (testCodeStr == null || testCodeStr.isEmpty()) return;

            int testCode;
            try {
                testCode = Integer.parseInt(testCodeStr);
            } catch (NumberFormatException e) {
                view.showMessage("Test ID musí byť číslo.");
                return;
            }

            if (model.findTest(testCode) != null) {
                view.showMessage("Test s týmto ID už existuje.");
                return;
            }

            String pid = view.promptInput("Zadaj ID osoby (napr. P1):");
            if (pid == null || pid.isEmpty()) return;

            Person person = model.findPerson(pid);
            if (person == null) {
                view.showMessage("Osoba neexistuje.");
                return;
            }

            if (person.getTestCount() >= 6) {
                view.showMessage("Osoba má už maximum testov (6).");
                return;
            }

            int year = Integer.parseInt(view.promptInput("Rok (napr. 2024):"));
            int month = Integer.parseInt(view.promptInput("Mesiac (1-12):"));
            int day = Integer.parseInt(view.promptInput("Deň (1-31):"));
            int hour = Integer.parseInt(view.promptInput("Hodina (0-23):"));
            int minute = Integer.parseInt(view.promptInput("Minúta (0-59):"));

            String resultStr = view.promptInput("Výsledok (true=pozitívny, false=negatívny):");
            if (resultStr == null || resultStr.isEmpty()) return;
            boolean positive = Boolean.parseBoolean(resultStr);

            String valueStr = view.promptInput("Hodnota (1-100):");
            if (valueStr == null || valueStr.isEmpty()) return;
            double value;
            try {
                value = Double.parseDouble(valueStr);
                if (value < 1 || value > 100) {
                    view.showMessage("Hodnota musí byť medzi 1-100.");
                    return;
                }
            } catch (NumberFormatException e) {
                view.showMessage("Hodnota musí byť číslo.");
                return;
            }

            String note = view.promptInput("Poznámka (max 10 znakov):");
            if (note == null) note = "";

            long timestamp = PCRTest.makeTimestamp(year, month, day, hour, minute);

            PCRTest test = new PCRTest(
                    testCode,
                    pid,
                    timestamp,
                    positive,
                    value,
                    note
            );

            if (!model.insertTest(test)) {
                view.showMessage("Test sa nepodarilo vložiť do databázy.");
                return;
            }

            if (!model.addTestToPerson(pid, testCode)) {
                view.showMessage("Test vložený, ale nepodarilo sa aktualizovať osobu.");
                return;
            }

            view.appendOutput("Test úspešne vložený!\n");
            view.appendOutput("   " + test + "\n");
            view.appendOutput("   Osoba " + pid + " má teraz " + (person.getTestCount() + 1) + " testov.\n");

        } catch (NumberFormatException e) {
            view.showMessage("Chyba: Neplatné číslo!");
        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void opFindPerson() {
        try {
            String id = view.promptInput("Zadaj ID osoby:");
            if (id == null || id.isEmpty()) return;

            Person p = model.findPerson(id);

            if (p == null) {
                view.showMessage("Osoba sa nenašla.");
            } else {
                view.appendOutput("═══════════════════════════════════");
                view.appendOutput("Nájdená osoba: " + p);
                view.appendOutput("═══════════════════════════════════");

                if (p.getTestCount() == 0) {
                    view.appendOutput("(žiadne testy)");
                } else {
                    view.appendOutput("TESTY:");
                    for (int testCode : p.getTestCodes()) {
                        PCRTest t = model.findTest(testCode);
                        if (t != null) {
                            view.appendOutput("- " + t);
                        }
                    }
                }
            }

        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
        }
    }

    private void opFindTest() {
        String codeStr = view.promptInput("Zadaj ID testu:");
        if (codeStr == null || codeStr.isEmpty()) return;

        int code;
        try {
            code = Integer.parseInt(codeStr);
        } catch (NumberFormatException e) {
            view.showMessage("ID testu musí byť číslo.");
            return;
        }

        PCRTest t = model.findTest(code);
        if (t == null)
            view.showMessage("Test sa nenašiel.");
        else
            view.appendOutput("Nájdený test: " + t);
    }

    private void opInsertPerson() {
        try {
            String id = view.promptInput("ID:");
            if (id == null || id.isEmpty()) return;

            String name = view.promptInput("Meno:");
            if (name == null || name.isEmpty()) return;

            String surname = view.promptInput("Priezvisko:");
            if (surname == null || surname.isEmpty()) return;

            int year = Integer.parseInt(view.promptInput("Rok narodenia:"));
            int month = Integer.parseInt(view.promptInput("Mesiac narodenia:"));
            int day = Integer.parseInt(view.promptInput("Deň narodenia:"));

            Person p = new Person(name, surname, id, year, month, day);

            if (model.insertPerson(p)) {
                view.appendOutput("Osoba vložená: " + p);
            } else {
                view.showMessage("Osoba sa nepodarilo vložiť.");
            }

        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
        }
    }

    private void opDeleteTest() {
        String codeStr = view.promptInput("ID testu:");
        if (codeStr == null || codeStr.isEmpty()) return;

        int code = Integer.parseInt(codeStr);

        if (model.deleteTest(code))
            view.appendOutput("Vymazaný test: " + code);
        else
            view.showMessage("Test sa nepodarilo vymazať.");
    }

    private void opDeletePerson() {
        String id = view.promptInput("Zadaj ID osoby:");
        if (id == null || id.isEmpty()) return;

        if (model.deletePerson(id))
            view.appendOutput("Osoba vymazaná: " + id);
        else
            view.showMessage("Osobu sa nepodarilo vymazať.");
    }

    private void opEditPerson() {
        String oldId = view.promptInput("ID osoby na editáciu:");
        if (oldId == null || oldId.isEmpty()) return;

        Person old = model.findPerson(oldId);
        if (old == null) {
            view.showMessage("Osoba neexistuje.");
            return;
        }

        try {
            String newName = view.promptInput("Nové meno:");
            String newSurname = view.promptInput("Nové priezvisko:");
            int y = Integer.parseInt(view.promptInput("Nový rok:"));
            int m = Integer.parseInt(view.promptInput("Nový mesiac:"));
            int d = Integer.parseInt(view.promptInput("Nový deň:"));

            Person updated = new Person(newName, newSurname, oldId, y, m, d);

            if (model.editPerson(oldId, updated))
                view.appendOutput("Osoba zmenená: " + updated);
            else
                view.showMessage("Nepodarilo sa editovať osobu.");

        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
        }
    }

    private void opEditTest() {
        try {
            String oldCodeStr = view.promptInput("ID testu na editáciu:");
            if (oldCodeStr == null || oldCodeStr.isEmpty()) return;

            int oldCode = Integer.parseInt(oldCodeStr);

            PCRTest old = model.findTest(oldCode);
            if (old == null) {
                view.showMessage("Test neexistuje.");
                return;
            }

            view.appendOutput("Aktuálny test: " + old + "\n");

            String resultStr = view.promptInput("Nový výsledok (true=pozitívny, false=negatívny):");
            if (resultStr == null || resultStr.isEmpty()) return;
            boolean positive = Boolean.parseBoolean(resultStr);

            int year = Integer.parseInt(view.promptInput("Rok (napr. 2024):"));
            int month = Integer.parseInt(view.promptInput("Mesiac (1-12):"));
            int day = Integer.parseInt(view.promptInput("Deň (1-31):"));
            int hour = Integer.parseInt(view.promptInput("Hodina (0-23):"));
            int minute = Integer.parseInt(view.promptInput("Minúta (0-59):"));

            String valueStr = view.promptInput("Nová hodnota (1-100):");
            if (valueStr == null || valueStr.isEmpty()) return;
            double value = Double.parseDouble(valueStr);
            if (value < 1 || value > 100) {
                view.showMessage("Hodnota musí byť medzi 1-100.");
                return;
            }

            String note = view.promptInput("Poznámka (max 10 znakov):");
            if (note == null) note = "";

            long timestamp = PCRTest.makeTimestamp(year, month, day, hour, minute);

            PCRTest updated = new PCRTest(
                    oldCode,
                    old.getPatientId(),
                    timestamp,
                    positive,
                    value,
                    note
            );

            if (model.editTest(oldCode, updated)) {
                view.appendOutput("Test zmenený: " + updated + "\n");
            } else {
                view.showMessage("Nepodarilo sa editovať test.");
            }

        } catch (NumberFormatException e) {
            view.showMessage("Chyba: Neplatné číslo!");
        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void onPrintPeople() {
        if (!model.isDbOpen()) {
            view.showMessage("Najprv otvor alebo vytvor databázu!");
            return;
        }
        view.showOutput(model.printPeople());
    }

    public void onPrintTests() {
        if (!model.isDbOpen()) {
            view.showMessage("Najprv otvor alebo vytvor databázu!");
            return;
        }
        view.showOutput(model.printTests());
    }

}