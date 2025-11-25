import Presenter.HeapFilePresenter;
import View.HeapFileView;
import Model.Person;
import DataStructures.HeapFile;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        deleteOldFiles();

        runPreGuiTest();

        javax.swing.SwingUtilities.invokeLater(() -> {
            try {
                HeapFileView view = new HeapFileView();
                HeapFilePresenter presenter = new HeapFilePresenter(view);
                view.setPresenter(presenter);

                view.setVisible(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void deleteOldFiles() {
        try {
            new File("data.bin").delete();
        } catch (Exception e) {
        }
    }

    private static void runPreGuiTest() {
        try {
            HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());
            Tester.runHeapFileTest(hf, 60, 30, 10);
            hf.close();
        } catch (Exception e) {
        }
    }
}
