import Presenter.HeapFilePresenter;
import Presenter.LinearHashFilePresenter;

import View.HeapFileView;
import View.LinearHashFileView;

import Model.Person;
import DataStructures.HeapFile;
import DataStructures.LinearHashFile;

import java.io.File;

public class Main {

    private static final boolean TEST_HEAPFILE = false;
    private static final boolean TEST_LINEAR_HASH = true;
    private static final boolean USE_TESTER = true;   // ← sem si dáš ON/OFF

    public static void main(String[] args) {

        deleteFiles();

        if (TEST_HEAPFILE) {
            runHeapGUI();
        }
        else if (TEST_LINEAR_HASH) {
            runLinearHashGUI();
        }
    }

    // ==============================================
    // HEAPFILE GUI
    // ==============================================
    private static void runHeapGUI() {

        javax.swing.SwingUtilities.invokeLater(() -> {
            try {

                HeapFile<Person> hf = new HeapFile<>("data.bin", 256, new Person());

                // tester
                if (USE_TESTER) {
                    Tester.mixedHeapTest(hf, 2000, 60, 20, 20, 50);
                }

                HeapFileView view = new HeapFileView();
                HeapFilePresenter presenter = new HeapFilePresenter(view, hf);

                view.setPresenter(presenter);
                view.setVisible(true);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // ==============================================
    // LINEAR HASHFILE GUI
    // ==============================================
    private static void runLinearHashGUI() {

        javax.swing.SwingUtilities.invokeLater(() -> {
            try {

                LinearHashFile<Person> lhf =
                        new LinearHashFile<>("data.bin", 256,
                                "overflow.bin", 256,
                                4,
                                new Person());

                // tester
                /*if (USE_TESTER) {
                    Tester.mixedLinearHashTest(
                            lhf,
                            8000,  // operations
                            50,    // inserts
                            25,    // deletes
                            25,    // finds
                            100    // validate interval
                    );
                }*/

                if (USE_TESTER) {
                    Tester.simpleInsertFindTest(lhf, 100, 1000);
                    //Tester.megaTest();

                }

                LinearHashFileView view = new LinearHashFileView();
                LinearHashFilePresenter presenter =
                        new LinearHashFilePresenter(view, lhf);

                view.setPresenter(presenter);
                view.setVisible(true);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // ==============================================
    // DELETE FILES
    // ==============================================
    private static void deleteFiles() {
        delete("data.bin");
        delete("data.bin.meta");
        delete("data.bin.lh.meta");     // ✅ PRIDAJ TOTO!
        delete("overflow.bin");
        delete("overflow.bin.meta");
    }
    private static void delete(String path) {
        try { new File(path).delete(); } catch (Exception ignored) {}
    }
}
