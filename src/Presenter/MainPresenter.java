package Presenter;

import Model.MainModel;
import Model.Person;
import Model.PCRTest;
import View.IMainView;

import java.util.ArrayList;

public class MainPresenter {

    private final IMainView view;
    private final MainModel model;

    public MainPresenter(IMainView view, MainModel model) {
        this.view = view;
        this.model = model;
        view.setPresenter(this);
    }

    // ─────────────────────────────────────────
    // TOP BUTTONS
    // ─────────────────────────────────────────

    /*
    public void onGenerateData() {
        view.appendOutput("Generujem 20 osôb + 20 testov...");

        var persons = model.generatePersons(20);

        for (Person p : persons) {
            PCRTest t = model.randomTest(p.getId());
            model.insertTest(t);
        }

        view.appendOutput("Hotovo.\n");
    }*/


    public void onGenerateData() {
        try {
            int persons = 100;
            int tests   = 200;
            long seed   = System.currentTimeMillis();

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
        model.close();
        view.appendOutput("Súbory uložené. Program ukončený.");
    }

    // ─────────────────────────────────────────
    // TASK BUTTON DISPATCH
    // ─────────────────────────────────────────

    public void onTaskSelected(int taskId) {
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

    // ─────────────────────────────────────────
    // 1. INSERT TEST
    // ─────────────────────────────────────────

    /*
    private void opInsertTest() {
        try {
            // 1️⃣ Test Code
            String testCodeStr = view.promptInput("Zadaj ID testu (číslo):");
            if (testCodeStr == null || testCodeStr.isEmpty()) return;

            int testCode;
            try {
                testCode = Integer.parseInt(testCodeStr);
            } catch (NumberFormatException e) {
                view.showMessage("❌ Test ID musí byť číslo.");
                return;
            }

            // ✅ Skontroluj, či test s týmto ID už existuje
            if (model.findTest(testCode) != null) {
                view.showMessage("❌ Test s týmto ID už existuje.");
                return;
            }

            // 2️⃣ ID osoby
            String pid = view.promptInput("Zadaj ID osoby (napr. P1):");
            if (pid == null || pid.isEmpty()) return;

            Person person = model.findPerson(pid);
            if (person == null) {
                view.showMessage("❌ Osoba neexistuje.");
                return;
            }

            // 3️⃣ Kontrola maxima testov
            if (person.getTestCount() >= 6) {
                view.showMessage("❌ Osoba má už maximum testov (6).");
                return;
            }

            // 4️⃣ Výsledok testu
            String resultStr = view.promptInput("Výsledok (true=pozitívny, false=negatívny):");
            if (resultStr == null || resultStr.isEmpty()) return;
            boolean positive = Boolean.parseBoolean(resultStr);

            // 5️⃣ Hodnota testu
            String valueStr = view.promptInput("Hodnota (1-100):");
            if (valueStr == null || valueStr.isEmpty()) return;
            int value;
            try {
                value = Integer.parseInt(valueStr);
                if (value < 1 || value > 100) {
                    view.showMessage("❌ Hodnota musí byť medzi 1-100.");
                    return;
                }
            } catch (NumberFormatException e) {
                view.showMessage("❌ Hodnota musí byť číslo.");
                return;
            }

            // 6️⃣ Poznámka
            String note = view.promptInput("Poznámka (max 10 znakov):");
            if (note == null) note = "";

            // 7️⃣ Vytvor test
            PCRTest test = new PCRTest(
                    testCode,
                    pid,
                    System.currentTimeMillis(),
                    positive,
                    value,
                    note
            );

            // 8️⃣ Vlož test do databázy
            if (!model.insertTest(test)) {
                view.showMessage("❌ Test sa nepodarilo vložiť do databázy.");
                return;
            }

            // 9️⃣ KRITICKÉ: Pridaj testCode do Person + ulož
            if (!model.addTestToPerson(pid, testCode)) {
                view.showMessage("⚠️ Test vložený, ale nepodarilo sa aktualizovať osobu.");
                return;
            }

            // 🔟 Úspech!
            view.appendOutput("✅ Test úspešne vložený!\n");
            view.appendOutput("   " + test + "\n");
            view.appendOutput("   Osoba " + pid + " má teraz " + (person.getTestCount() + 1) + " testov.\n");

        } catch (Exception e) {
            view.showMessage("❌ Chyba: " + e.getMessage());
            e.printStackTrace();
        }
    }*/

    private void opInsertTest() {
        try {
            // 1️⃣ Test Code
            String testCodeStr = view.promptInput("Zadaj ID testu (číslo):");
            if (testCodeStr == null || testCodeStr.isEmpty()) return;

            int testCode;
            try {
                testCode = Integer.parseInt(testCodeStr);
            } catch (NumberFormatException e) {
                view.showMessage("❌ Test ID musí byť číslo.");
                return;
            }

            // ✅ Skontroluj, či test s týmto ID už existuje
            if (model.findTest(testCode) != null) {
                view.showMessage("❌ Test s týmto ID už existuje.");
                return;
            }

            // 2️⃣ ID osoby
            String pid = view.promptInput("Zadaj ID osoby (napr. P1):");
            if (pid == null || pid.isEmpty()) return;

            Person person = model.findPerson(pid);
            if (person == null) {
                view.showMessage("❌ Osoba neexistuje.");
                return;
            }

            // 3️⃣ Kontrola maxima testov
            if (person.getTestCount() >= 6) {
                view.showMessage("❌ Osoba má už maximum testov (6).");
                return;
            }

            // 4️⃣ DÁTUM A ČAS - manuálne zadávanie
            int year = Integer.parseInt(view.promptInput("Rok (napr. 2024):"));
            int month = Integer.parseInt(view.promptInput("Mesiac (1-12):"));
            int day = Integer.parseInt(view.promptInput("Deň (1-31):"));
            int hour = Integer.parseInt(view.promptInput("Hodina (0-23):"));
            int minute = Integer.parseInt(view.promptInput("Minúta (0-59):"));

            // 5️⃣ Výsledok testu
            String resultStr = view.promptInput("Výsledok (true=pozitívny, false=negatívny):");
            if (resultStr == null || resultStr.isEmpty()) return;
            boolean positive = Boolean.parseBoolean(resultStr);

            // 6️⃣ Hodnota testu
            String valueStr = view.promptInput("Hodnota (1-100):");
            if (valueStr == null || valueStr.isEmpty()) return;
            double value;
            try {
                value = Double.parseDouble(valueStr);
                if (value < 1 || value > 100) {
                    view.showMessage("❌ Hodnota musí byť medzi 1-100.");
                    return;
                }
            } catch (NumberFormatException e) {
                view.showMessage("❌ Hodnota musí byť číslo.");
                return;
            }

            // 7️⃣ Poznámka
            String note = view.promptInput("Poznámka (max 10 znakov):");
            if (note == null) note = "";

            // 8️⃣ Vytvor test - NOVÝ KONŠTRUKTOR s rozdelenými poliami
            PCRTest test = new PCRTest(
                    testCode,
                    pid,
                    year, month, day, hour, minute,  // ✅ Manuálne zadané
                    positive,
                    value,
                    note
            );

            // 9️⃣ Vlož test do databázy
            if (!model.insertTest(test)) {
                view.showMessage("❌ Test sa nepodarilo vložiť do databázy.");
                return;
            }

            // 🔟 KRITICKÉ: Pridaj testCode do Person + ulož
            if (!model.addTestToPerson(pid, testCode)) {
                view.showMessage("⚠️ Test vložený, ale nepodarilo sa aktualizovať osobu.");
                return;
            }

            // 1️⃣1️⃣ Úspech!
            view.appendOutput("✅ Test úspešne vložený!\n");
            view.appendOutput("   " + test + "\n");
            view.appendOutput("   Osoba " + pid + " má teraz " + (person.getTestCount() + 1) + " testov.\n");

        } catch (NumberFormatException e) {
            view.showMessage("❌ Chyba: Neplatné číslo!");
        } catch (Exception e) {
            view.showMessage("❌ Chyba: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ─────────────────────────────────────────
    // 2. FIND PERSON
    // ─────────────────────────────────────────

    private void opFindPerson() {
        String id = view.promptInput("Zadaj ID osoby:");
        if (id == null || id.isEmpty()) return;

        Person p = model.findPerson(id);

        if (p == null) {
            view.showMessage("Osoba sa nenašla.");
        } else {
            view.appendOutput("Nájdená osoba: " + p);
        }
    }

    // ─────────────────────────────────────────
    // 3. FIND TEST
    // ─────────────────────────────────────────

    private void opFindTest() {
        String codeStr = view.promptInput("Zadaj ID testu:");
        if (codeStr == null || codeStr.isEmpty()) return;

        int code;
        try { code = Integer.parseInt(codeStr); }
        catch (NumberFormatException e) {
            view.showMessage("ID testu musí byť číslo.");
            return;
        }

        PCRTest t = model.findTest(code);
        if (t == null)
            view.showMessage("Test sa nenašiel.");
        else
            view.appendOutput("Nájdený test: " + t);
    }

    // ─────────────────────────────────────────
    // 4. INSERT PERSON
    // ─────────────────────────────────────────

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

    // ─────────────────────────────────────────
    // 5. DELETE TEST
    // ─────────────────────────────────────────

    private void opDeleteTest() {
        String codeStr = view.promptInput("ID testu:");
        if (codeStr == null || codeStr.isEmpty()) return;

        int code = Integer.parseInt(codeStr);

        if (model.deleteTest(code))
            view.appendOutput("Vymazaný test: " + code);
        else
            view.showMessage("Test sa nepodarilo vymazať.");
    }

    // ─────────────────────────────────────────
    // 6. DELETE PERSON
    // ─────────────────────────────────────────

    private void opDeletePerson() {
        String id = view.promptInput("Zadaj ID osoby:");
        if (id == null || id.isEmpty()) return;

        if (model.deletePerson(id))
            view.appendOutput("Osoba vymazaná: " + id);
        else
            view.showMessage("Osobu sa nepodarilo vymazať.");
    }

    // ─────────────────────────────────────────
    // 7. EDIT PERSON
    // ─────────────────────────────────────────

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

    // ─────────────────────────────────────────
    // 8. EDIT TEST
    // ─────────────────────────────────────────

    /*
    private void opEditTest() {
        String oldCodeStr = view.promptInput("ID testu na editáciu:");
        if (oldCodeStr == null || oldCodeStr.isEmpty()) return;

        int oldCode = Integer.parseInt(oldCodeStr);

        PCRTest old = model.findTest(oldCode);
        if (old == null) {
            view.showMessage("Test neexistuje.");
            return;
        }

        try {
            String pid = view.promptInput("Nový patientID:");
            boolean positive = Boolean.parseBoolean(view.promptInput("Pozitívny? true/false:"));
            long time = Long.parseLong(view.promptInput("Nový timestamp:"));
            int value = Integer.parseInt(view.promptInput("Nová hodnota:"));
            String note = view.promptInput("Poznámka:");

            PCRTest updated = new PCRTest(oldCode, pid, time, positive, value, note);

            if (model.editTest(oldCode, updated))
                view.appendOutput("Test zmenený: " + updated);
            else
                view.showMessage("Nepodarilo sa editovať test.");

        } catch (Exception e) {
            view.showMessage("Chyba: " + e.getMessage());
        }
    }*/

    private void opEditTest() {
        try {
            // 1️⃣ ID testu na editáciu
            String oldCodeStr = view.promptInput("ID testu na editáciu:");
            if (oldCodeStr == null || oldCodeStr.isEmpty()) return;

            int oldCode = Integer.parseInt(oldCodeStr);

            PCRTest old = model.findTest(oldCode);
            if (old == null) {
                view.showMessage("Test neexistuje.");
                return;
            }

            // ⚠️ ZOBRAZ AKTUÁLNE HODNOTY
            view.appendOutput("Aktuálny test: " + old + "\n");

            // 2️⃣ VÝSLEDOK (true/false)
            String resultStr = view.promptInput("Nový výsledok (true=pozitívny, false=negatívny):");
            if (resultStr == null || resultStr.isEmpty()) return;
            boolean positive = Boolean.parseBoolean(resultStr);

            // 3️⃣ DÁTUM A ČAS - rozdelené polia
            int year = Integer.parseInt(view.promptInput("Rok (napr. 2024):"));
            int month = Integer.parseInt(view.promptInput("Mesiac (1-12):"));
            int day = Integer.parseInt(view.promptInput("Deň (1-31):"));
            int hour = Integer.parseInt(view.promptInput("Hodina (0-23):"));
            int minute = Integer.parseInt(view.promptInput("Minúta (0-59):"));

            // 4️⃣ HODNOTA (1-100)
            String valueStr = view.promptInput("Nová hodnota (1-100):");
            if (valueStr == null || valueStr.isEmpty()) return;
            int value = Integer.parseInt(valueStr);
            if (value < 1 || value > 100) {
                view.showMessage("❌ Hodnota musí byť medzi 1-100.");
                return;
            }

            // 5️⃣ POZNÁMKA
            String note = view.promptInput("Poznámka (max 10 znakov):");
            if (note == null) note = "";

            // 6️⃣ VYTVOR NOVÝ TEST (s novým konštruktorom)
            PCRTest updated = new PCRTest(
                    oldCode,
                    old.getPatientId(),  // ⚠️ PatientId sa NESMIE meniť!
                    year, month, day, hour, minute,
                    positive,
                    value,
                    note
            );

            // 7️⃣ ULOŽ
            if (model.editTest(oldCode, updated)) {
                view.appendOutput("✅ Test zmenený: " + updated + "\n");
            } else {
                view.showMessage("Nepodarilo sa editovať test.");
            }

        } catch (NumberFormatException e) {
            view.showMessage("❌ Chyba: Neplatné číslo!");
        } catch (Exception e) {
            view.showMessage("❌ Chyba: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void onPrintPeople() {
        view.showOutput(model.printPeople());
    }

    public void onPrintTests() {
        view.showOutput(model.printTests());
    }


    public void onRandomPrint() {
        Person p = model.getRandomPersonSimple();
        if (p == null) {
            view.appendOutput("⚠️ V databáze nie sú žiadne osoby.\n");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("===== NÁHODNÁ OSOBA =====\n");
        sb.append("ID: ").append(p.getId()).append("\n");
        sb.append("Meno: ").append(p.getName()).append("\n");
        sb.append("Priezvisko: ").append(p.getSurname()).append("\n");
        sb.append("Rok nar.: ").append(p.getYear()).append("\n");
        sb.append("Mesiac: ").append(p.getMonth()).append("\n");
        sb.append("Deň: ").append(p.getDay()).append("\n\n");

        sb.append("===== JEJ TESTY =====\n");

        // ✅ Načítaj LEN testy tejto osoby
        ArrayList<PCRTest> tests = model.getPersonTests(p.getId());

        if (tests.isEmpty()) {
            sb.append("(žiadne testy)\n");
        } else {
            for (PCRTest t : tests) {
                sb.append(t).append("\n");
            }
        }

        view.showOutput(sb.toString());
    }


}
