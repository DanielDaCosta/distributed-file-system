
#import dash_html_components as html
from dash import html
#import dash_core_components as dcc
from dash import dcc
from dash.dependencies import Input, Output
from apps import navigation
import dash
from enum import Enum
from dash import dcc
import pandas as pd
import numpy as np
import pmr.pmr as pmr
import edfs.firebase as file_system

# Pmr Configuration
# class syntax

file_name = "Consume_Price_Index"
data = file_system.read_dataset(f"root/user/{file_name}")
dataset_list = ['Consume_Price_Index', 'Per_Capita_GDP']

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
                        html.Div(children="EDFS", className="menu-title"),
                        dcc.Dropdown(
                            id="edfs-filter",
                            options=['Firebase', 'MySQL', 'MongoDB'],
                            value="Firebase",
                            clearable=False,
                            placeholder="Select Indicator",
                            searchable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Br(),
                html.Div(
                    children=[
                        html.Div(children="Filename", className="menu-title"),
                        dcc.Dropdown(
                            id="dataset-filter",
                            # options=['Stats_Cap_Ind_Sample', 'Human_Capital_Index', 'Stats_Cap_Ind_Sample_2'],
                            # value="Stats_Cap_Ind_Sample",
                            clearable=False,
                            placeholder="Select Indicator",
                            searchable=False,
                            className="dropdown",
                        ),
                    ],
                    style={"margin-left": "15px"}
                ),
                html.Br()
            ],
            className='menu3'
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Country", className="menu-title"),
                        dcc.Dropdown(
                            id="region-filter",
                            # value="Afghanistan",
                            placeholder="Select Country",
                            clearable=True,
                            className="dropdown",
                            multi=True
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(children="Type", className="menu-title"),
                        dcc.Dropdown(id="type-filter", clearable=True, placeholder="Select Indicator",
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
                                for agg_filter in ['sum', 'max', 'min', 'avg']
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
        html.Div(id='dummy'),
        html.Div(id='dummy-2')
    # html.H3(children="Graph of Data"),
    # dcc.Link('Home',href="/"),
    # html.Br(),
    # dcc.Link('model-showcase',href="/showcase")
])

# Change EDFS
@dash.callback(
    Output("dataset-filter", "options"),
    # Output("region-filter", "options"), 
    # Output("type-filter", "options"),
    Input("edfs-filter", "value"),
)
def change_edfs(edfs_filter):
    global file_system
    global data
    global dataset_list
    if edfs_filter == "MySQL":
        import edfs.mysql as file_system
        dataset_list = ['Access_Electricity', 'Access_Fuels']
    elif edfs_filter == "MongoDB":
        import edfs.mongodb as file_system
        dataset_list = ['GDP_Growth']
    else:
        import edfs.firebase as file_system
        dataset_list = ['Consume_Price_Index', 'Per_Capita_GDP']
    return dataset_list#, [], []


@dash.callback(
    Output("region-filter", "options"),
    Output("type-filter", "options"),
    Input("dataset-filter", "value"),
    Input("edfs-filter", "value"),
)
def select_dataset(dataset_name, edfs_filter):
    global data
    global file_name
    if dataset_name:
        print(dataset_name)
        file_name = dataset_name
        if edfs_filter == "MySQL" and file_name in dataset_list:
            data = file_system.read_dataset(f"/root/user/{file_name}")
        elif edfs_filter == "MongoDB":
            data = file_system.read_dataset(f"datasets/{file_name}.csv")
        else:
            data = file_system.read_dataset(f"root/user/{file_name}")
        
    
    coutry_name_list = [ {"label": country_name, "value": country_name}
        for country_name in np.sort(data.Country_Name.unique())
    ]

    series_name_list = [ {"label": series_name, "value": series_name} for series_name in data.Series_Name.unique()]
    return coutry_name_list, series_name_list


@dash.callback(
    Output("price-chart", "figure"),
    [
        Input("region-filter", "value"),
        Input("type-filter", "value"),
        # Input("date-range", "start_date"),
        # Input("date-range", "end_date"),
        Input("date-range", "value"),
        # Input("agg-filter", "value"),
    ],
)
def update_charts(country_name, series_name, date_range):

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

    # # If agregation function is selected
    # if agg_filter:
    #     filtered_data = filtered_data\
    #             .groupby('Year').agg({'Value': agg_filter})\
    #                 .reset_index().assign(Country_Name='Selected Countries')


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

    # volume_chart_figure = {
    #     "data": [
    #         {
    #             "x": filtered_data["Year"],
    #             "y": filtered_data["Value"],
    #             "type": "lines",
    #         },
    #     ],
    #     "layout": {
    #         "title": {
    #             "text": f"{series_name}",
    #             "x": 0.05,
    #             "xanchor": "left"
    #         },
    #         "xaxis": {"fixedrange": True},
    #         "yaxis": {"fixedrange": True},
    #         "colorway": ["#E12D39"],
    #     },
    # }
    return price_chart_figure




@dash.callback(
    Output("volume-chart", "figure"),
    [
        Input("edfs-filter", "value"),
        Input("dataset-filter", "value"),
        Input("type-filter", "value"),
        Input("date-range", "value"),
        Input("agg-filter", "value"),
    ],
)
def update_charts_agg(edfs_filter, dataset_filter, series_name, date_range, agg_filter):

    if edfs_filter and dataset_filter and series_name and date_range and agg_filter:
        map_agg_filter = {
            'sum': "SUM",
            'max': "MAX",
            'min': "MIN",
            'avg': "AVG"
        }
        AGG_FILTER = map_agg_filter[agg_filter]
        YEAR_RANGE = [f"{year} [YR{year}]" for year in range(int(date_range[0]), int(date_range[1]))]  # convert year to "{year}[YR{year}]"
        # print(edfs_filter)
        if edfs_filter == 'MySQL':
            # print("MYSQL", AGG_FILTER, YEAR_RANGE, f"/root/foo/{dataset_filter}")
            data_agg = pmr.execute("MYSQL", AGG_FILTER, targets=YEAR_RANGE, file=f"/root/user/{dataset_filter}", DEBUG=True)
        elif edfs_filter == 'MongoDB':
            data_agg = pmr.execute("MONGODB", AGG_FILTER, targets=YEAR_RANGE, file=f"/root/foo/{dataset_filter}", DEBUG=True)
        else:
            data_agg = pmr.execute("FIREBASE", AGG_FILTER, targets=YEAR_RANGE, file=f"root/user/{dataset_filter}", DEBUG=True)
        
        volume_chart_figure = {
            "data": [{
                "x": data_agg["Year"],
                "y": data_agg["Value"],
                "type": "lines",
                "hovertemplate": "%{y:.2f}<extra></extra>",
                "name": f"{AGG_FILTER}"
            }],
            "layout": {
                "title": {
                    "text": f"{AGG_FILTER} for {series_name}",
                    "x": 0.05,
                    "xanchor": "left",
                },
                "xaxis": {"fixedrange": True},
                # "yaxis": {"tickprefix": "$", "fixedrange": True},
                "yaxis": {"fixedrange": True},
                # "colorway": ["#17B897"],
            }
        }
        return volume_chart_figure
    return {}