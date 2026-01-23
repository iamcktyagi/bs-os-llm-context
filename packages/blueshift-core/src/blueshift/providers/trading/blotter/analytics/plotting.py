from __future__ import annotations
from typing import TYPE_CHECKING

try:
    import calendar
    from jinja2 import Environment, BaseLoader  # type: ignore -> optional dependency
    import plotly.graph_objects as go           # type: ignore -> optional dependency
    from plotly import io                       # type: ignore -> optional dependency
    from plotly.subplots import make_subplots   # type: ignore -> optional dependency
    USE_PLOTLY=True
except ImportError:
    USE_PLOTLY=False

if TYPE_CHECKING:
    import pandas as pd
    import calendar
    from jinja2 import Environment, BaseLoader  # type: ignore -> optional dependency
    import plotly.graph_objects as go           # type: ignore -> optional dependency
    from plotly import io                       # type: ignore -> optional dependency
    from plotly.subplots import make_subplots   # type: ignore -> optional dependency 

def import_empyrical():
    try:
        import empyrical.stats as empst         # type: ignore -> optional dependency
        return empst
    except ImportError:
        from blueshift.lib.common.sentinels import nan_op
        class EMPST():
            def aggregate_returns(self, *args, **kwargs):
                return nan_op(*args, **kwargs)

        empst = EMPST()
        return empst
    
PLOTLY_TEMPLATE = 'simple_white'

MAIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div>
        {{ timeseries }}
    </div>
</body>
</html>
"""

MAIN_TEMPLATE_DIV_ONLY = """
<div>
    <div>
        {{ timeseries }}
    </div>
</div>
"""

TEAR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div>
        {{ timeseries }}
    </div>
    <div>
        {{ distribution }}
    </div>
    <div>
        {{ heatmap }}
    </div>
    <div>
        {{ positions }}
    </div>
    <div>
        {{ pertrade }}
    </div>
    <div>
        {{ record }}
    </div>
</body>
</html>
"""

TEAR_TEMPLATE_DIV_ONLY = """
<div>
    <div>
        {{ timeseries }}
    </div>
    <div>
        {{ distribution }}
    </div>
    <div>
        {{ heatmap }}
    </div>
    <div>
        {{ positions }}
    </div>
    <div>
        {{ pertrade }}
    </div>
    <div>
        {{ record }}
    </div>
</div>
"""

def get_main_plot(perfs:pd.DataFrame, include_plotlyjs:bool=False, return_div:bool=True,
                  theme:str='light'):
    """ Generate html version of a chart from `perfs`. """
    if not USE_PLOTLY:
        return

    fig = plot_main(perfs, theme=theme)
    div = io.to_html(fig, full_html=False, include_plotlyjs=False)

    if return_div:
        template = Environment(loader=BaseLoader).from_string(MAIN_TEMPLATE_DIV_ONLY) # nosec
    else:
        template = Environment(loader=BaseLoader).from_string(MAIN_TEMPLATE) # nosec

    template_vars = {"timeseries":div}
    html = template.render(template_vars)

    return html

def save_main_plot(perfs:pd.DataFrame, target:str, width:int = 960, height:int = 560, margin_top:int=60, 
                   margin_right:int=20, margin_bottom:int=20, margin_left:int=50, theme:str='light'):
    """ generate the plot from `perfs` and save to the path `target`. """
    if not USE_PLOTLY:
        return
    
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'

    fig = plot_main(perfs, theme=theme)

    fig.update_layout(
            template=theme,
            margin=dict(
                    l=margin_left,
                    r=margin_right,
                    t=margin_top,
                    b=margin_bottom)
            )

    fig.update_layout(
            legend=dict(
                    yanchor="top",
                    bgcolor='#e5ecf6',
                    y=0.99,
                    xanchor="left",
                    x=0.01))

    fig.write_image(target, width=width,
                    height=height)

def get_tear_sheet(perfs:pd.DataFrame, pos:pd.DataFrame|None=None, per_trade:pd.DataFrame|None=None,
                    records:pd.DataFrame|None=None, return_div:bool=True, theme:str='light'):
    if not USE_PLOTLY:
        return

    timeseries = plot_strategy_timeseries(perfs, theme=theme)
    distribution = plot_distribution(perfs, theme=theme)
    heatmap = plot_heatmap(perfs, theme=theme)
    positions = plot_strategy_positions(perfs, pos, theme=theme)
    pertrade = plot_per_trade(per_trade, theme=theme)
    record = plot_strategy_records(perfs, records, theme=theme)

    timeseries = io.to_html(
            timeseries, full_html=False, include_plotlyjs=False)

    distribution = io.to_html(
            distribution, full_html=False, include_plotlyjs=False)
    heatmap = io.to_html(
            heatmap, full_html=False, include_plotlyjs=False)

    template_vars = {"timeseries":timeseries,
                     'distribution':distribution,
                     'heatmap':heatmap,
                     }

    if positions:
        positions = io.to_html(
                positions, full_html=False, include_plotlyjs=False)
        template_vars['positions'] = positions

    if pertrade:
        pertrade = io.to_html(
                pertrade, full_html=False, include_plotlyjs=False)
        template_vars['pertrade'] = pertrade

    if record:
        record = io.to_html(
                record, full_html=False, include_plotlyjs=False)
        template_vars['record'] = record

    if return_div:
        template = Environment(loader=BaseLoader).from_string(TEAR_TEMPLATE_DIV_ONLY) # nosec
    else:
        template = Environment(loader=BaseLoader).from_string(TEAR_TEMPLATE) # nosec

    html = template.render(template_vars)

    return html

def plot_main(perfs:pd.DataFrame, theme:str='ligth'):
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    
    fig = go.Figure()
    fig.add_trace(
            go.Scatter(
                    x=perfs.index, y=perfs.pnls, mode='lines',
                    name='Strategy Profit/Loss'),)

    if 'benchmark_pnls' in perfs.columns:
        fig.add_trace(
                go.Scatter(
                        x=perfs.index, y=perfs.benchmark_pnls,
                        mode='lines',
                        name='Benchmark Performance'))

    fig.update_layout(
            template=theme,
            title_text="Strategy vs. Benchmark Performance",
            legend=dict(
                yanchor="top",
                bgcolor='#e5ecf6',
                y=0.99,
                xanchor="left",
                x=0.01)
            )

    return fig

def plot_strategy_timeseries(perfs:pd.DataFrame, theme:str='light'):
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    
    def col_display(col):
        return ' '.join(col.split('_')).capitalize()

    cols = list(perfs.columns)
    cols.pop(cols.index('net'))

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
            go.Scatter(
                    x=perfs.index,
                    y=perfs.net,
                    mode='lines',
                    name="Strategy Net"),
            secondary_y=False)

    for col in cols:
        fig.add_trace(
                go.Scatter(
                        x=perfs.index, y=perfs[col],
                        mode='lines', name=col, visible=False),
                secondary_y=True)

    buttons = []

    for col in cols:
        idx = cols.index(col)
        visibility = [True] + [False for c in cols]
        visibility[idx+1] = True
        buttons.append(dict(
            method='update',label=col,
            args=[{"visible":visibility}]
        ))

    drop_down = {'active':0, 'buttons':buttons,
                 'direction':'down', 'showactive':True,
                 'xanchor':'left', 'yanchor':'top',
                 'x':0.35, 'y':1.2}

    fig.update_layout(
            template=theme,
            title_text="Strategy vs. Performance Metrics",
            showlegend=True,
            updatemenus=[drop_down])

    fig.update_yaxes(
            title_text="Total Strategy Value", secondary_y=False)
    fig.update_yaxes(
            title_text="Performance Metric", secondary_y=True)

    return fig

def plot_strategy_positions(perfs:pd.DataFrame, pos:pd.DataFrame|None=None, MAX_POSITIONS:int=500,
                            theme:str='light'):
    if pos is None or pos.empty:
        return
    
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    cols = list(pos.columns)[-MAX_POSITIONS:]

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
            go.Scatter(
                    x=perfs.index,
                    y=perfs.net,
                    mode='lines',
                    name="Strategy Net"),
            secondary_y=False)

    for col in cols:
        fig.add_trace(
                go.Scatter(
                        x=pos.index, y=pos[col],
                        mode='lines', fill='tozeroy',
                        name=col),
                secondary_y=True)

    buttons = []
    visibility = [True] + [True for c in cols]
    buttons.append(dict(
            method='update',label='All',
            args=[{"visible":visibility}]))

    for col in cols:
        idx = cols.index(col)
        visibility = [True] + [False for c in cols]
        visibility[idx+1] = True
        buttons.append(dict(
            method='update',label=col,
            args=[{"visible":visibility}]
        ))

    drop_down = {'active':0, 'buttons':buttons, 'direction':'down',
                 'showactive':True, 'xanchor':'left',
                 'yanchor':'top','x':0.35, 'y':1.2}

    fig.update_layout(
            template=theme,
            title_text="Strategy vs. Positions",
            showlegend=True,
            updatemenus=[drop_down])

    fig.update_yaxes(
            title_text="Total Strategy Value", secondary_y=False)
    fig.update_yaxes(
            title_text="Asset Position", secondary_y=True)

    return fig

def plot_distribution(perfs:pd.DataFrame, theme:str='light'):
    def col_display(col):
        return ' '.join(col.split('_')).capitalize()
    
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    buttons = []
    cols = ['algo_returns','day_pnl', 'gross_leverage',
                    'net_leverage', 'algo_volatility']
    for col in cols:
        buttons.append(dict(
            method='restyle',label=col_display(col), visible=True,
            args=[{'x':[perfs[col]], 'type':'histogram'}]
        ))

    drop_down = {'buttons':buttons, 'direction':'down',
                 'showactive':True, 'xanchor':'left',
                 'yanchor':'top','x':0.35, 'y':1.2}

    fig = go.Figure()
    fig.add_trace(go.Histogram(x=perfs.algo_returns))
    fig.update_layout(template=theme, title_text="Metrics Distribution",
                      showlegend=False, updatemenus=[drop_down])

    return fig

def plot_per_trade(per_trade:pd.DataFrame|None=None, theme:str='light'):
    if per_trade is None or per_trade.empty:
        return
    
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    per_trade.index.name = 'Metrics'
    per_trade = per_trade.reset_index()
    
    values = [per_trade[col] for col in per_trade.columns]
    table = go.Figure(data=[go.Table(
        header=dict(values=list(per_trade.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=values,
                   fill_color='lavender',
                   align='left'))
    ])
        
    table.update_layout(
            template=theme,
            title_text="Per Trade Metrics",
            )
        
    return table


def plot_strategy_records(perfs:pd.DataFrame, records:pd.DataFrame|None=None, theme:str='light'): 
    def col_display(col):
        return ' '.join(col.split('_')).capitalize()
    
    def autoscale(layout, xrange, scaler=0.01):
        in_view = perfs.net.loc[
                fig.layout.xaxis.range[0]:fig.layout.xaxis.range[1]]
        y0, y1 = in_view.min()*(1-scaler), in_view.max()*(1+scaler)
        fig.update_yaxes(range=[y0, y1], secondary_y=False)
        
        in_view = records.loc[fig.layout.xaxis.range[0]:fig.layout.xaxis.range[1]] # type: ignore
        y0, y1 = in_view.min().min()*(1-scaler), in_view.max().max()*(1+scaler)
        fig.update_yaxes(range=[y0, y1], secondary_y=True)
    
    if records is None or records.empty:
        return
    
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    cols = list(records.columns)
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
            go.Scatter(
                    x=perfs.index, 
                    y=perfs.net, 
                    mode='lines', 
                    name="Strategy Net"), 
            secondary_y=False)
            
    for col in cols:
        name = col_display(col)
        fig.add_trace(
                go.Scatter(
                        x=records.index, y=records[col], 
                        mode='lines', name=name, visible=True), 
                secondary_y=True)
    
    fig.update_layout(
            template=theme,
            title_text="Strategy vs. Recorded Variables", 
            showlegend=True,
            )
    
    fig.update_yaxes(
            title_text="Total Strategy Value", secondary_y=False)
    fig.update_yaxes(
            title_text="Recorded Variable", secondary_y=True)
    
    fig.update_layout(
        xaxis=dict(
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )
    
    fig.layout.on_change(autoscale, 'xaxis.range')
    
    return fig

def plot_heatmap(perfs:pd.DataFrame, freq:str='monthly', theme:str='light'):
    empst = import_empyrical()
    theme = 'plotly_dark' if theme=='dark' else 'plotly_white'
    monthly = empst.aggregate_returns(perfs.algo_returns,freq)
    monthly.index = monthly.index.set_names(['year', 'month'])  # type: ignore
    monthly = monthly.reset_index()                             # type: ignore
    monthly = monthly.pivot(
            index='year', columns='month', values='algo_returns')
    
    monthly.columns = [calendar.month_abbr[c] for c in monthly.columns]
    monthly.index = ['y'+str(y) for y in monthly.index] 
    
    fig = go.Figure(data=go.Heatmap(
                       z=monthly.values,
                       x=monthly.columns,
                       y=monthly.index,
                       colorscale = 'RdYlGn'))
    
    fig.update_layout(
            template=theme,
            title_text="Monthly Returns Heatmap",
            )
    
    return fig

    