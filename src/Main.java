import Presenter.HeapFilePresenter;
import View.HeapFileView;
import Model.Person;
import DataStructures.HeapFile;

import java.io.File;
public class Main {

    private static final boolean USE_TESTER = true;

    public static void main(String[] args) {

        deleteOldFiles();

        HeapFile<Person> sharedHF = null;

        if (USE_TESTER) {
            sharedHF = runTest();
        }

        final HeapFile<Person> finalHF = sharedHF;

        javax.swing.SwingUtilities.invokeLater(() -> {
            try {
                HeapFileView view = new HeapFileView();
                HeapFilePresenter presenter;

                if (finalHF != null) {
                    presenter = new HeapFilePresenter(view, finalHF);
                } else {
                    presenter = new HeapFilePresenter(view);
                }

                view.setPresenter(presenter);
                view.setVisible(true);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void deleteOldFiles() {
        try { new File("data.bin").delete(); } catch (Exception ignored) { }
        try { new File("data.bin.meta").delete(); } catch (Exception ignored) { }
    }

    private static HeapFile<Person> runTest() {
        try {
            HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());
            Tester.runHeapFileTest(hf, 1000, 500, 400,10);
            return hf;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
