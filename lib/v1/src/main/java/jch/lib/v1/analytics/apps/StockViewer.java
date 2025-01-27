package jch.lib.analytics.apps;
import java.time.LocalTime;
import java.util.ArrayList;

//import javafx.application.Application;
import javafx.stage.Stage;
import jch.lib.analytics.investment.nyse.xdp.TaqMsg;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgFactory;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType003;
import jch.lib.analytics.investment.nyse.xdp.TaqMsgType100;
import jch.lib.analytics.investment.stock.StatEntry;
import jch.lib.analytics.investment.stock.StatFrame;
import jch.lib.analytics.investment.stock.StockDay;
import javafx.scene.Scene;
import javafx.scene.chart.*;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
//import javafx.scene.shape.Rectangle;
//import javafx.scene.control.*; 
//import javafx.scene.layout.*; 
//import javafx.event.ActionEvent; 
//import javafx.event.EventHandler; 
import javafx.collections.*;
import javafx.scene.chart.XYChart.Data;
import javafx.scene.chart.XYChart.Series;

public class StockViewer extends javafx.application.Application{

	
	@Override
	public void init() throws Exception {
		// TODO Auto-generated method stub
		super.init();
	}

	@Override
	public void start(Stage stage) throws Exception {
		// TODO Auto-generated method stub

		
		tLabel = new Label("this is a test");
		tStackPane = new StackPane();
		tStackPane.getChildren().add(createScatterChart());
		tStackPane.getChildren().add(tLabel);
		
		tScene = new Scene(tStackPane,700,500);
		
		stage.setScene(tScene);
		
		stage.show();
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		super.stop();
	}

	public StockViewer() {

	}
	
	
	//https://www.developer.com/java/data/working-with-javafx-chart-apis.html
	//http://myjavafx.blogspot.com/2013/09/javafx-charts-display-date-values-on.html
	@SuppressWarnings("unchecked")
	XYChart<NumberAxis, NumberAxis> createScatterChart() {
		
		
		String fileLoc = "D:\\Inv\\20191007\\";
		String fileName = "EQY_US_AMEX_IBF_1_20191007.txt";
		ArrayList<TaqMsg> nyseDay = TaqMsgFactory.loadFile(fileLoc + fileName);
		fileName = "EQY_US_AMEX_IBF_2_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_3_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_4_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_5_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		fileName = "EQY_US_AMEX_IBF_6_20191007.txt";
		nyseDay.addAll(TaqMsgFactory.loadFile(fileLoc + fileName));
		
		StockDay aal = new StockDay("AAL", nyseDay);
		
		double prevPrice = 0;
		int prevVol = 0;
		//get previous price
		LocalTime startTime = LocalTime.parse("09:30:00");
		LocalTime stopTime = LocalTime.parse("16:00:00");
		for(TaqMsgType003 msg: aal.SymbolIndexMappingsMsgs) {
			prevPrice = msg.getPrevClosePrice();
			prevVol = msg.getPrevCloseVolume();
		}
		
		
		ObservableList<XYChart.Series<Double, Double>> data =
		         FXCollections.observableArrayList();
		Series<Double, Double> aalSeries = new Series<>();
		StatFrame aalStats = new StatFrame();
		StatEntry initEntry = new StatEntry(startTime, prevPrice, prevVol);
		aalStats.addEntry(initEntry);
		for(TaqMsgType100 msg : aal.AddOrderMsgs) {	
			if(msg.getSide().equals("B")) {
				StatEntry entry = new StatEntry(msg.getSourceTime(), msg.getPrice(), msg.getVolume());
				aalStats.addEntry(entry);				
				aalSeries.getData().add(new Data<>(entry.calcTimeHour(),entry.getPrice()));	
			}
		}
		aalStats.addEntry(new StatEntry(stopTime, aalStats.getLastEntry().getPrice(), 0));
		aalStats.prepTimeDeltas();
		
		System.out.println("Loaded");
		
		data.add(aalSeries);
				
		NumberAxis xAxis = new NumberAxis("hr",
				aalStats.getFirstEntry().getTime().getHour() + 1,
				aalStats.getLastEntry().getTime().getHour(),
				1);
	
		NumberAxis yAxis = new NumberAxis("price", 
				aalStats.calcMinPrice() - .01,
				aalStats.calcMaxPrice() + .01,
				.05);
		
		@SuppressWarnings("rawtypes")
		LineChart sc = new LineChart(xAxis, yAxis);
		
		sc.setData(data);
		sc.setCreateSymbols(false);
		
		return sc;
	}

	
	Label tLabel = null;
	StackPane tStackPane = null;
	Scene tScene = null;
	
	@SuppressWarnings("rawtypes")
	ScatterChart tChart = null;

}
