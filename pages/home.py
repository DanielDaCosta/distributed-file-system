#import dash_html_components as html
from dash import html, callback
#import dash_core_components as dcc
from dash import dcc
from dash.dependencies import Input, Output
from apps import navigation
import dash
import base64
from edfs.firebase import ls

# Load Folder and File Icons
folder_icon = 'images/folder_image.png' # replace with your own image
file_icon = 'images/file_image.png' # replace with your own image
folder_image = base64.b64encode(open(folder_icon, 'rb').read()).decode('ascii')
file_image = base64.b64encode(open(file_icon, 'rb').read()).decode('ascii')

# Load EDFS Path
edfs_full_path = ""


dash.register_page(
    __name__,
    path='/'
)

# layout = html.Div(children=[
#     navigation.navbar,
#     html.H3(children="testing"),

    
#     dcc.Link(
#         html.Div([
#             html.Img(
#                 src='data:image/png;base64,{}'.format(folder_image),
#                 width=50,
#                 id='analytics-input')
#         ]), href="/?var=1"),
#     # html.Img(src='data:image/png;base64,{}'.format(file_image), width=50),
#     # html.Img(src=dash.get_asset_url('my-image.png'))
    
#     # dcc.Link('Home',href="/"),
#     # html.Br(),
#     # dcc.Link('model-showcase',href="/showcase")
#     # html.Div(id='analytics-output')
# ])

def path_edfs_components(edfs_components: str) -> list:
    global edfs_full_path
    edfs_components = edfs_components.split(', ')
    result_list = []
    for component in edfs_components:
        if '/' in component:
            result_list.append(
            dcc.Link(
                html.Div([
                    html.Img(
                        src='data:image/png;base64,{}'.format(folder_image),
                        style={"width":"50px"}
                    ),
                    html.P(f'{component}', style={'textAlign': 'center'})
                ], style={'width': '20%', 'textAlign': 'center', 'display': 'inline-block'}), href=f"/?full_path={edfs_full_path + component}")
            )
        elif component == 'empty':
                result_list.append(html.Div([
                        html.P('Empty Folder', style={'textAlign': 'center'})
                    ], style={'width': '20%', 'display': 'inline-block'}))
        else:
            result_list.append(
                    html.Div([
                        html.Img(
                            src='data:image/png;base64,{}'.format(file_image),
                            style={"width":"50px"}
                        ),
                        html.P(f'{component}', style={'textAlign': 'center'})
                    ], style={'width': '20%', 'textAlign': 'center', 'display': 'inline-block'})
            )
    return result_list

def layout(full_path="", **other_unknown_query_strings):

    # Reference global variable
    global edfs_full_path
    # print(edfs_full_path)
    edfs_full_path = full_path

    # Get folders in EDFS directory
    edfs_components = ls(edfs_full_path)
    # print(edfs_components)

    folders_structure = path_edfs_components(edfs_components)


    # folders_html = [
    #     dcc.Link(
    #         html.Div([
    #             html.Img(
    #                 src='data:image/png;base64,{}'.format(file_image),
    #                 style={"width":"50px"}
    #             ),
    #             html.P(f'{variable_a}', style={'textAlign': 'center'})
    #         ], style={'display': 'inline-block'}), href=f"/?full_path={variable_a + 1}&files={variable_a - 1}")
    # ]*variable_a

    # print(folders_structure)
    output = [navigation.navbar, html.Br()] + folders_structure
    return html.Div(
	    children=output
    # [
    #     navigation.navbar,
	#     html.H1(children='This is our Archive page'),

	#     html.Div(children=f'''
	#         This is report: {folders}.\n
	# 		This is department: {files}.
	#     '''),
    #     dcc.Link('Home', href=f"/?folders={variable_a + 1}&files={variable_a - 1}")

	# ]
    )

# @callback(
#     Output(component_id='analytics-input', component_property='children'),
#     Input(component_id='analytics-input', component_property='n_clicks')
# )
# def update_city_selected(input_value):
#     # return html.Img(src='data:image/png;base64,{}'.format(file_image), width=50)
#     if input_value:
#         re
        # return html.Div(
        #     html.Div([
        #         html.Img(
        #             src='data:image/png;base64,{}'.format(file_image),
        #             width=50,
        #             id='analytics-input')
        #     ])
            
        #     # html.Img(src='data:image/png;base64,{}'.format(file_image), width=50),
        #     # html.Img(src=dash.get_asset_url('my-image.png'))
            
        #     # dcc.Link('Home',href="/"),
        #     # html.Br(),
        #     # dcc.Link('model-showcase',href="/showcase")
        #     # html.Div(id='analytics-output')
        # )
        # return         html.Img(
        #     src='data:image/png;base64,{}'.format(folder_image),
        #     width=50)