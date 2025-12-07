package View;

import Presenter.MainPresenter;
import java.util.*;

public interface IMainView {
    void setPresenter(MainPresenter presenter);
    void showOutput(String text);
    void appendOutput(String text);
    String promptInput(String message);
    void showMessage(String message);
    void showTable(String title, String[] columns, List<Object[]> rows);
}