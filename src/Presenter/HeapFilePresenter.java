package Presenter;

import DataStructures.HeapFile;
import Model.Person;
import View.HeapFileView;

import javax.swing.*;
import java.util.Random;

public class HeapFilePresenter {

    private final HeapFile<Person> heapFile;
    private final HeapFileView view;
    private final Random rnd = new Random();

    private static final int GEN_COUNT = 100;
    private int idCounter = 0;

    public HeapFilePresenter(HeapFileView view) throws Exception {
        this.view = view;
        this.heapFile = new HeapFile<>("data.bin", 256, new Person());
    }

    public void onInsertRandom() {
        try {
            Person p = randomPerson();
            long addr = heapFile.insert(p);
            view.append("Inserted: " + p + " on addr=" + addr);
        } catch (Exception e) {
            error("Insert failed", e);
        }
    }

    public void onGet() {
        try {
            String id = JOptionPane.showInputDialog("Enter ID:");
            if (id == null || id.isEmpty()) return;

            String addrStr = JOptionPane.showInputDialog("Enter block address:");
            if (addrStr == null || addrStr.isEmpty()) return;

            long addr = Long.parseLong(addrStr);

            Person p = new Person();
            p.fromId(id);

            Person found = heapFile.get(addr, p);

            if (found != null)
                view.append("FOUND at addr=" + addr + ": " + found);
            else
                view.append("NOT FOUND at addr=" + addr + ": " + id);

        } catch (Exception e) {
            error("GET failed", e);
        }
    }

    public void onDelete() {
        try {
            String id = JOptionPane.showInputDialog("Enter ID to delete:");
            if (id == null || id.isEmpty()) return;

            String addrStr = JOptionPane.showInputDialog("Enter block address:");
            if (addrStr == null || addrStr.isEmpty()) return;

            long addr = Long.parseLong(addrStr);

            Person p = new Person();
            p.fromId(id);

            boolean ok = heapFile.delete(addr, p);

            if (ok) {
                view.append("DELETED at addr=" + addr + ": " + id);
            }
            else {
                view.append("NOT FOUND for delete at addr=" + addr + ": " + id);
            }

        } catch (Exception e) {
            error("DELETE failed", e);
        }
    }

    public void onGenerateBatch() {
        try {
            view.append("\n=== GENERATING " + GEN_COUNT + " PERSONS ===");

            for (int i = 0; i < GEN_COUNT; i++) {
                Person p = randomPerson();
                long addr = heapFile.insert(p);
                view.append("Inserted: " + p + " on addr=" + addr);
            }

            view.append("=== DONE ===\n");

        } catch (Exception e) {
            error("Data generation failed", e);
        }
    }

    public void onPrint() {
        try {
            view.append("\n===== HEAPFILE CONTENT =====");
            String dump = heapFile.print();
            view.append(dump);
        } catch (Exception e) {
            error("Print failed", e);
        }
    }

    public void onClose() {
        try {
            heapFile.close();
            view.append("HeapFile closed.");
        } catch (Exception e) {
            error("Close failed", e);
        }
    }

    private void error(String msg, Exception e) {
        e.printStackTrace();
        view.append("ERROR: " + e.getMessage());
        JOptionPane.showMessageDialog(null, msg + ":\n" + e.getMessage(),
                "Error", JOptionPane.ERROR_MESSAGE);
    }



    private String randomString(Random rnd, int length) {
        String chars = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }

        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));

        return sb.toString();
    }


    private Person randomPerson() {

        String name = randomString(rnd, 6);
        String surname = randomString(rnd, 8);

        String id = "ID" + (idCounter++);

        int year = 1980 + rnd.nextInt(30);
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);

        return new Person(name, surname, id, year, month, day);
    }



}
