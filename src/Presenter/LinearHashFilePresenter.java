package Presenter;

import DataStructures.LinearHashFile;
import Model.Person;
import View.LinearHashFileView;

import javax.swing.*;
import java.util.Random;

public class LinearHashFilePresenter {

    private final LinearHashFile<Person> hashFile;
    private final LinearHashFileView view;
    private final Random rnd = new Random();

    private static final int GEN_COUNT = 100;
    private int idCounter = 0;

    public LinearHashFilePresenter(LinearHashFileView view,
                                   LinearHashFile<Person> hashFile) {
        this.view = view;
        this.hashFile = hashFile;
    }

    public void onInsert() {
        try {
            Person p = randomPerson();
            hashFile.insert(p);
            view.append("INSERTED: " + p);
        } catch (Exception e) {
            error("Insert failed", e);
        }
    }

    public void onFind() {
        try {
            String id = JOptionPane.showInputDialog("Enter ID:");
            if (id == null || id.isEmpty()) return;

            Person pattern = new Person();
            pattern.fromId(id);

            Person found = hashFile.find(pattern);

            if (found != null)
                view.append("FOUND: " + found);
            else
                view.append("NOT FOUND: " + id);

        } catch (Exception e) {
            error("Find failed", e);
        }
    }

    public void onUpdate() {
        try {
            String id = JOptionPane.showInputDialog("Enter ID to update:");
            if (id == null || id.isEmpty()) return;

            Person pattern = new Person();
            pattern.fromId(id);

            Person newData = randomPerson();

            boolean ok = hashFile.update(pattern, newData);

            if (ok)
                view.append("UPDATED: " + id + " → " + newData);
            else
                view.append("NOT FOUND: " + id);

        } catch (Exception e) {
            error("Update failed", e);
        }
    }

    public void onDelete() {
        try {
            String id = JOptionPane.showInputDialog("Enter ID to delete:");
            if (id == null || id.isEmpty()) return;

            Person pattern = new Person();
            pattern.fromId(id);

            boolean ok = hashFile.delete(pattern);

            if (ok)
                view.append("DELETED: " + id);
            else
                view.append("NOT FOUND: " + id);

        } catch (Exception e) {
            error("Delete failed", e);
        }
    }

    public void onPrint() {
        try {
            view.append("\n===== LinearHashFile CONTENT =====");
            view.append(hashFile.print());
        } catch (Exception e) {
            error("Print failed", e);
        }
    }

    public void onClose() {
        try {
            hashFile.close();
            view.append("HashFile closed.");
        } catch (Exception e) {
            error("Close failed", e);
        }
    }

    private void error(String msg, Exception e) {
        e.printStackTrace();
        JOptionPane.showMessageDialog(null,
                msg + "\n" + e.getMessage(),
                "Error", JOptionPane.ERROR_MESSAGE);
    }

    private Person randomPerson() {
        String id = "ID" + (idCounter++);
        return new Person(
                randomString(6),
                randomString(8),
                id,
                1980 + rnd.nextInt(30),
                1 + rnd.nextInt(12),
                1 + rnd.nextInt(28)
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
}
