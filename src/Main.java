import Presenter.HeapFilePresenter;
import View.HeapFileView;

import java.io.File;

public class Main {
    public static void main(String[] args) {

        // 🔥 ZMAŽ STARÉ SÚBORY PRED SPUSTENÍM
        try {
            File f1 = new File("data.bin");
            File f2 = new File("data.bin.meta");

            if (f1.exists()) f1.delete();
            if (f2.exists()) f2.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 🔥 Spustenie GUI
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
