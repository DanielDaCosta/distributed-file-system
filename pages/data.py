
#import dash_html_components as html
from dash import html
#import dash_core_components as dcc
from dash import dcc
from dash.dependencies import Input, Output
from apps import navigation
import dash

from dash import dcc
import pandas as pd
import numpy as np

# data = pd.read_csv("avocado.csv")
# data = data.query("type == 'conventional' and region == 'Albany'")
# data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
# data.sort_values("Date", inplace=True)
df = pd.read_csv("datasets/Data_Extract_From_Statistical_Capacity_Indicators/42377300-c075-4554-a55f-41cd64c79126_Data.csv")
# function to get year columns
def is_year (c):
    return any(char.isdigit() for char in c)

# change columns names
new_columns = list()
columns = df.columns
for c in columns:
    if is_year(c):
        new_columns.append(c[:4])
    else:
        new_columns.append(c.replace(" ","_"))

# change column names in dataframe
df.columns = new_columns
df = df.drop(columns=['Country_Code', 'Series_Code'], axis=1)
data = df.melt(id_vars=["Country_Name", "Series_Name"], 
        var_name="Year", 
        value_name="Value")
data['Year'] = data['Year'].astype(int)
data = data.loc[data['Value'] != '..'].copy()
data['Value'] = data['Value'].astype(float)



dash.register_page(
    __name__,
    path='/data'
)


layout = html.Div(children=[
    navigation.navbar,
            html.Div(
            children=[
                html.P(children="🔎", className="header-emoji"),
                html.H1(
                    children="Search and Analytics", className="header-title"
                ),
                html.P(
                    children="Partition-based map and reduce (PMR) on data stored on EDFS",
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Country", className="menu-title"),
                        dcc.Dropdown(
                            id="region-filter",
                            options=[
                                {"label": country_name, "value": country_name}
                                for country_name in np.sort(data.Country_Name.unique())
                            ],
                            # value="Afghanistan",
                            placeholder="Select Country",
                            clearable=True,
                            className="dropdown",
                            multi=True
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="Type", className="menu-title"),
                        dcc.Dropdown(
                            id="type-filter",
                            options=[
                                {"label": series_name, "value": series_name}
                                for series_name in data.Series_Name.unique()
                            ],
                            value="Child malnutrition",
                            clearable=False,
                            placeholder="Select Indicator",
                            searchable=False,
                            className="dropdown",
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(
                            children="Date Range",
                            className="menu-title"
                            ),
                        # dcc.DatePickerRange(
                        #     id="date-range",
                        #     min_date_allowed=data.Date.min().date(),
                        #     max_date_allowed=data.Date.max().date(),
                        #     start_date=data.Date.min().date(),
                        #     end_date=data.Date.max().date(),
                        # ),
                        dcc.RangeSlider(
                            min=2000, max=2020, id='date-range', value = [2000,2020],
                            marks = {
                                2000: {'label': '2000', 'style': {'color': '#77b0b1'}},
                                2005: {'label': '2005'},
                                2010: {'label': '2010'},
                                2015: {'label': '2015'},
                                2020: {'label': '2020', 'style': {'color': '#f50'}}
                            }
                        ),
                    ],
                    style={'width': '30%'}
                ),
                html.Div(
                    children=[
                        html.Div(children="Aggregate", className="menu-title"),
                        dcc.Dropdown(
                            id="agg-filter",
                            options=[
                                {"label": agg_filter, "value": agg_filter}
                                for agg_filter in ['mean', 'max', 'min', 'median']
                            ],
                            # value="N/A",
                            placeholder="Select Agg Function",
                            clearable=True,
                            searchable=True,
                            className="dropdown",
                        ),
                    ],
                )
            ],
            className="menu",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="price-chart", config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="volume-chart", config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    # html.H3(children="Graph of Data"),
    # dcc.Link('Home',href="/"),
    # html.Br(),
    # dcc.Link('model-showcase',href="/showcase")
])

@dash.callback(
    [Output("price-chart", "figure"), Output("volume-chart", "figure")],
    [
        Input("region-filter", "value"),
        Input("type-filter", "value"),
        # Input("date-range", "start_date"),
        # Input("date-range", "end_date"),
        Input("date-range", "value"),
        Input("agg-filter", "value"),
    ],
)
def update_charts(country_name, series_name, date_range, agg_filter):

    if country_name:
        mask = (
            (data.Country_Name.isin(country_name))
            & (data.Series_Name == series_name)
            & (data.Year >= int(date_range[0]))
            & (data.Year <= int(date_range[1]))
        )
    else:
        mask = (
            (data.Series_Name == series_name)
            & (data.Year >= int(date_range[0]))
            & (data.Year <= int(date_range[1]))
        )
    filtered_data = data.loc[mask, :]

    # If agregation function is selected
    if agg_filter:
        filtered_data = filtered_data\
                .groupby('Year').agg({'Value': agg_filter})\
                    .reset_index().assign(Country_Name='Selected Countries')


    data_list = []
    for country_name in filtered_data.Country_Name.unique():
        df_temp = filtered_data[filtered_data['Country_Name'] == country_name]
        dict_append = {
            "x": df_temp["Year"],
            "y": df_temp["Value"],
            "type": "lines",
            "hovertemplate": "%{y:.2f}<extra></extra>",
            "name": country_name
        }
        data_list.append(dict_append)
    price_chart_figure = {
        "data": data_list,
        "layout": {
            "title": {
                "text": f"{series_name}",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            # "yaxis": {"tickprefix": "$", "fixedrange": True},
            "yaxis": {"fixedrange": True},
            # "colorway": ["#17B897"],
        },
    }

    volume_chart_figure = {
        "data": [
            {
                "x": filtered_data["Year"],
                "y": filtered_data["Value"],
                "type": "lines",
            },
        ],
        "layout": {
            "title": {
                "text": f"{series_name}",
                "x": 0.05,
                "xanchor": "left"
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"fixedrange": True},
            "colorway": ["#E12D39"],
        },
    }
    return price_chart_figure, volume_chart_figure