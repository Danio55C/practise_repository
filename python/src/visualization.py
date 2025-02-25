import pandas as pd
import seaborn as sns
from pandas.plotting import table 
import matplotlib
import matplotlib.pyplot as plt
import dataframe_image as dfi



def create_table_png(data):
    data_df= pd.DataFrame(data).set_index("Alertid")
    ##print(enriched_data_df)
    vizualization_table_path = "/usr/app/output/data_frame.png"
    dfi.export(data_df, vizualization_table_path, table_conversion="matplotlib", dpi=300)


def create_bar_plot(data):
    alert_counts_df= pd.DataFrame(data).set_index("Alertid")
    alert_counts_df = alert_counts_df.groupby("AlertName")["SeverityLevel"].value_counts().reset_index(name="Count")

    plt.figure(figsize=(20, 16))
    sns.barplot(data=alert_counts_df, x="AlertName", y="Count", hue="SeverityLevel", native_scale=True, palette="flare", dodge=True, width=0.8).set(xlabel=None)

    plt.title("Number of alerts grouped by severity level" , fontsize=25)
    plt.ylabel("Count",loc="center",fontsize=22)
    plt.xticks(rotation=0,fontsize=14)
    plt.legend(title="Severity Level", title_fontsize=16, fontsize=14) 
    plt.savefig("/usr/app/output/top_alerts.png", dpi=300)
    plt.close()


def create_linear_plot(data):
    data_df= pd.DataFrame(data).set_index("Alertid")    
    data_df["Timestamp"] = pd.to_datetime(data_df["Timestamp"])

    plt.figure(figsize=(20, 10))
    alert_time_series = data_df.groupby(pd.Grouper(key="Timestamp", freq="h")).size()
    
    sns.lineplot(x=alert_time_series.index, y=alert_time_series.values, marker="o", linestyle="-", color="b")

    plt.title("Alert Frequency Over Time", fontsize=25)
    plt.xlabel("Timestamp", fontsize=22)
    plt.ylabel("Number of Alerts", fontsize=22)
    plt.xticks(fontsize=14)
    plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.gca().xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=4)) 

    plt.savefig("/usr/app/output/alert_frequency,png", dpi=300)
    plt.close()

