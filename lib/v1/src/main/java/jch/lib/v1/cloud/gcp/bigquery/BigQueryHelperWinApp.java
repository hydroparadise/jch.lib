package jch.lib.cloud.gcp.bigquery;

import javafx.application.Application;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Stage;



public class BigQueryHelperWinApp extends Application {
    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("BigQuery Helper Window");

        // Create parent BorderPane
        BorderPane parentPane = new BorderPane();

        // Create a VBox for the left panel with cascading lists
        VBox leftPanel = new VBox();
        for (int i = 1; i <= 3; i++) {
            ListView<String> listView = new ListView<>();
            for (int j = 1; j <= 5; j++) {
                listView.getItems().add("List " + i + " Item " + j);
            }
            leftPanel.getChildren().add(listView);
        }
        parentPane.setLeft(leftPanel);

        // Create a VBox for the right panel with a text box
        VBox rightPanel = new VBox();
        TextField textField = new TextField();
        textField.setPromptText("Enter text");
        rightPanel.getChildren().add(textField);
        rightPanel.setAlignment(Pos.CENTER);
        rightPanel.setMinWidth(300); // Set a minimum width for the right panel
        parentPane.setRight(rightPanel);

        // Create a GridPane for the bottom panel with a cell grid
        GridPane bottomPanel = new GridPane();
        for (int row = 0; row < 4; row++) {
            for (int col = 0; col < 4; col++) {
                Label cell = new Label("Cell " + (row * 4 + col));
                cell.setStyle("-fx-border-color: black;");
                bottomPanel.add(cell, col, row);
            }
        }
        parentPane.setCenter(bottomPanel);

        // Create an HBox for the buttons at the very bottom
        HBox buttonBox = new HBox();
        Button button1 = new Button("Button 1");
        Button button2 = new Button("Button 2");
        buttonBox.getChildren().addAll(button1, button2);
        buttonBox.setAlignment(Pos.CENTER);
        parentPane.setBottom(buttonBox);

        Scene scene = new Scene(parentPane, 800, 600);

        // Set parentPane to fill 90% of the window's height
        parentPane.prefHeightProperty().bind(scene.heightProperty().multiply(0.9));

        primaryStage.setScene(scene);
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}