"""
AdventureWorks DW — Dashboard Analítico
Construído com Plotly Dash + Dash Bootstrap Components

Instalação:
    pip install dash dash-bootstrap-components plotly pandas python-dotenv snowflake-connector-python

Execução:
    python dashboard/app.py
"""

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from dash import dcc, html

import data_loader as dl
import queries as q

# ── Paleta de Cores ────────────────────────────────────────────────────────────
BG          = "#0b0e1a"
CARD_BG     = "#131728"
CARD_BORDER = "#1e2340"
PURPLE      = "#7c3aed"
PURPLE_LIGHT= "#a78bfa"
CYAN        = "#06b6d4"
CYAN_LIGHT  = "#67e8f9"
GREEN       = "#10b981"
AMBER       = "#f59e0b"
RED         = "#ef4444"
TEXT        = "#e2e8f0"
TEXT_MUTED  = "#94a3b8"
GRID        = "#1e2340"

PIE_COLORS  = [PURPLE, CYAN, GREEN, AMBER, RED, "#8b5cf6", "#ec4899"]
FONT        = "Inter, 'Segoe UI', sans-serif"

CHART_LAYOUT = dict(
    paper_bgcolor=CARD_BG,
    plot_bgcolor=CARD_BG,
    font=dict(family=FONT, color=TEXT, size=12),
    margin=dict(t=40, b=40, l=40, r=20),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_MUTED)),
    xaxis=dict(gridcolor=GRID, linecolor=GRID, tickfont=dict(color=TEXT_MUTED)),
    yaxis=dict(gridcolor=GRID, linecolor=GRID, tickfont=dict(color=TEXT_MUTED)),
    colorway=PIE_COLORS,
)

# ── Carga de Dados ─────────────────────────────────────────────────────────────
df_kpi        = dl.load(q.KPI_GENERAL,          dl.sample_kpi)
df_monthly    = dl.load(q.MONTHLY_REVENUE,       dl.sample_monthly_revenue)
df_products   = dl.load(q.TOP_PRODUCTS,          dl.sample_top_products)
df_country    = dl.load(q.REVENUE_BY_COUNTRY,    dl.sample_revenue_by_country)
df_channel    = dl.load(q.SALES_CHANNEL,         dl.sample_sales_channel)
df_reasons    = dl.load(q.SALES_REASONS,         dl.sample_sales_reasons)
df_status     = dl.load(q.ORDER_STATUS,          dl.sample_order_status)
df_sellers    = dl.load(q.SELLERS_BY_REGION,     dl.sample_sellers_by_region)
df_category   = dl.load(q.REVENUE_BY_CATEGORY,  dl.sample_revenue_by_category)

kpi = df_kpi.iloc[0]


def fmt_currency(val: float) -> str:
    if val >= 1_000_000:
        return f"$ {val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"$ {val/1_000:.1f}K"
    return f"$ {val:,.2f}"


def fmt_number(val: float) -> str:
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val/1_000:.0f}K"
    return f"{int(val):,}"


# ── Helpers de Estilo ──────────────────────────────────────────────────────────
def kpi_card(title: str, value: str, subtitle: str = "", accent: str = PURPLE) -> dbc.Col:
    return dbc.Col(
        html.Div(
            [
                html.P(title, className="kpi-label"),
                html.H3(value, className="kpi-value", style={"color": accent}),
                html.P(subtitle, className="kpi-sub") if subtitle else None,
            ],
            className="kpi-card",
        ),
        xs=6, md=3,
    )


def section_title(text: str) -> html.Div:
    return html.Div(
        html.H5(text, style={"color": TEXT_MUTED, "fontWeight": "600",
                             "letterSpacing": "0.05em", "textTransform": "uppercase",
                             "fontSize": "0.75rem", "marginBottom": "1rem"}),
    )


def chart_card(figure: go.Figure, height: int = 360) -> html.Div:
    return html.Div(
        dcc.Graph(figure=figure, config={"displayModeBar": False},
                  style={"height": f"{height}px"}),
        className="chart-card",
    )


# ── Figuras ────────────────────────────────────────────────────────────────────

def fig_monthly_revenue() -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_monthly["order_month"],
        y=df_monthly["monthly_revenue"],
        mode="lines+markers",
        name="Receita",
        line=dict(color=PURPLE, width=2.5),
        marker=dict(size=5, color=PURPLE_LIGHT),
        fill="tozeroy",
        fillcolor="rgba(124,58,237,0.12)",
        hovertemplate="<b>%{x|%b %Y}</b><br>Receita: $ %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df_monthly["order_month"],
        y=df_monthly["monthly_orders"],
        name="Pedidos",
        yaxis="y2",
        marker_color="rgba(6,182,212,0.25)",
        hovertemplate="<b>%{x|%b %Y}</b><br>Pedidos: %{y:,}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Receita & Volume de Pedidos Mensais", font=dict(size=14, color=TEXT), x=0.01),
        yaxis=dict(**CHART_LAYOUT["yaxis"], title=None, tickprefix="$", tickformat=",.0f"),
        yaxis2=dict(overlaying="y", side="right", showgrid=False,
                    tickfont=dict(color=TEXT_MUTED), title=None),
        legend=dict(**CHART_LAYOUT["legend"], orientation="h",
                    y=-0.15, x=0.5, xanchor="center"),
        hovermode="x unified",
    )
    return fig


def fig_top_products() -> go.Figure:
    df = df_products.sort_values("total_revenue")
    fig = go.Figure(go.Bar(
        x=df["total_revenue"],
        y=df["product_name"],
        orientation="h",
        marker=dict(
            color=df["total_revenue"],
            colorscale=[[0, "#4c1d95"], [1, CYAN]],
            showscale=False,
        ),
        hovertemplate="<b>%{y}</b><br>Receita: $ %{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Top 10 Produtos por Receita", font=dict(size=14, color=TEXT), x=0.01),
        xaxis=dict(**CHART_LAYOUT["xaxis"], tickprefix="$", tickformat=",.0f"),
        yaxis=dict(**CHART_LAYOUT["yaxis"], tickfont=dict(size=10, color=TEXT_MUTED)),
        bargap=0.3,
    )
    return fig


def fig_revenue_by_country() -> go.Figure:
    df = df_country.sort_values("total_revenue", ascending=True)
    fig = go.Figure(go.Bar(
        x=df["total_revenue"],
        y=df["country"],
        orientation="h",
        marker_color=CYAN,
        hovertemplate="<b>%{y}</b><br>Receita: $ %{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Receita por País", font=dict(size=14, color=TEXT), x=0.01),
        xaxis=dict(**CHART_LAYOUT["xaxis"], tickprefix="$", tickformat=",.0f"),
    )
    return fig


def fig_sales_channel() -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=df_channel["channel"],
        values=df_channel["total_revenue"],
        hole=0.65,
        marker=dict(colors=[PURPLE, CYAN]),
        textinfo="label+percent",
        textfont=dict(color=TEXT),
        hovertemplate="<b>%{label}</b><br>Receita: $ %{value:,.0f}<br>Participação: %{percent}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Canal de Venda", font=dict(size=14, color=TEXT), x=0.01),
        showlegend=False,
        annotations=[dict(text="Canal", x=0.5, y=0.5, font_size=13,
                          font_color=TEXT_MUTED, showarrow=False)],
    )
    return fig


def fig_sales_reasons() -> go.Figure:
    row = df_reasons.iloc[0]
    labels = ["Preço", "Fabricante", "Qualidade", "Promoção", "Análise", "Outro", "TV"]
    keys   = ["price_count","manufacturer_count","quality_count",
              "promotion_count","review_count","other_count","television_count"]
    values = [int(row[k]) for k in keys]
    fig = go.Figure(go.Pie(
        labels=labels,
        values=values,
        hole=0.55,
        marker=dict(colors=PIE_COLORS),
        textinfo="label+percent",
        textfont=dict(color=TEXT, size=11),
        hovertemplate="<b>%{label}</b><br>Ocorrências: %{value:,}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Motivos de Compra", font=dict(size=14, color=TEXT), x=0.01),
        showlegend=False,
    )
    return fig


def fig_order_status() -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=df_status["status"],
        values=df_status["total_orders"],
        hole=0.55,
        marker=dict(colors=[GREEN, CYAN, PURPLE_LIGHT, RED, AMBER]),
        textinfo="label+percent",
        textfont=dict(color=TEXT, size=11),
        hovertemplate="<b>%{label}</b><br>Pedidos: %{value:,}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Status dos Pedidos", font=dict(size=14, color=TEXT), x=0.01),
        showlegend=False,
    )
    return fig


def fig_sellers_by_region() -> go.Figure:
    df = (df_sellers.groupby(["country", "gender"])["total_orders"]
          .sum().reset_index()
          .sort_values("total_orders", ascending=False))
    colors = {g: PURPLE if g == "M" else CYAN for g in df["gender"].unique()}
    fig = go.Figure()
    for gender, label, color in [("M", "Masculino", PURPLE), ("F", "Feminino", CYAN)]:
        sub = df[df["gender"] == gender]
        fig.add_trace(go.Bar(
            name=label,
            x=sub["country"],
            y=sub["total_orders"],
            marker_color=color,
            hovertemplate="<b>%{x}</b><br>Pedidos: %{y:,}<extra></extra>",
        ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Pedidos por País e Gênero do Vendedor", font=dict(size=14, color=TEXT), x=0.01),
        barmode="group",
        bargap=0.25,
        legend=dict(**CHART_LAYOUT["legend"], orientation="h", y=-0.18, x=0.5, xanchor="center"),
    )
    return fig


def fig_revenue_by_category() -> go.Figure:
    df = df_category.sort_values("total_revenue", ascending=False)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Receita",
        x=df["category"],
        y=df["total_revenue"],
        marker_color=PURPLE,
        hovertemplate="<b>%{x}</b><br>Receita: $ %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Custo",
        x=df["category"],
        y=df["total_cost"],
        marker_color=CYAN,
        hovertemplate="<b>%{x}</b><br>Custo: $ %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Lucro Bruto",
        x=df["category"],
        y=df["gross_profit"],
        marker_color=GREEN,
        hovertemplate="<b>%{x}</b><br>Lucro: $ %{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Receita, Custo e Lucro por Categoria", font=dict(size=14, color=TEXT), x=0.01),
        barmode="group",
        yaxis=dict(**CHART_LAYOUT["yaxis"], tickprefix="$", tickformat=",.0f"),
        legend=dict(**CHART_LAYOUT["legend"], orientation="h", y=-0.18, x=0.5, xanchor="center"),
    )
    return fig


# ── Layout ─────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
    title="AdventureWorks DW",
)

app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh", "fontFamily": FONT},
    children=[

        # ── Barra Superior ─────────────────────────────────────────────────────
        html.Div(
            dbc.Container(
                dbc.Row([
                    dbc.Col(
                        html.Div([
                            html.Span("◈ ", style={"color": PURPLE, "fontSize": "1.4rem"}),
                            html.Span("AdventureWorks DW", style={
                                "color": TEXT, "fontWeight": "700",
                                "fontSize": "1.1rem", "letterSpacing": "0.02em",
                            }),
                        ]),
                        width="auto",
                    ),
                    dbc.Col(
                        html.P(
                            "Dashboard Analítico de Vendas  ·  Snowflake + dbt + Airflow",
                            style={"color": TEXT_MUTED, "margin": 0, "fontSize": "0.8rem"},
                        ),
                        className="d-none d-md-flex align-items-center",
                    ),
                    dbc.Col(
                        html.Span("2011 – 2014", style={
                            "color": TEXT_MUTED, "fontSize": "0.75rem",
                            "border": f"1px solid {CARD_BORDER}",
                            "padding": "2px 10px", "borderRadius": "20px",
                        }),
                        width="auto", className="d-flex align-items-center",
                    ),
                ], align="center", justify="between"),
                fluid=True,
            ),
            style={
                "backgroundColor": CARD_BG,
                "borderBottom": f"1px solid {CARD_BORDER}",
                "padding": "14px 0",
                "position": "sticky", "top": "0", "zIndex": "100",
            },
        ),

        # ── Conteúdo Principal ─────────────────────────────────────────────────
        dbc.Container(
            [
                html.Div(style={"height": "28px"}),

                # ── KPIs ──────────────────────────────────────────────────────
                section_title("Visão Geral"),
                dbc.Row([
                    kpi_card("Receita Total",      fmt_currency(kpi["total_revenue"]),      "pedidos aprovados/enviados", PURPLE),
                    kpi_card("Total de Pedidos",   fmt_number(kpi["total_orders"]),          "",                           CYAN),
                    kpi_card("Clientes Únicos",    fmt_number(kpi["total_customers"]),       "",                           GREEN),
                    kpi_card("Ticket Médio",       fmt_currency(kpi["avg_order_value"]),    "",                           AMBER),
                ], className="g-3 mb-4"),

                dbc.Row([
                    kpi_card("Receita por Produto",fmt_currency(kpi["total_product_revenue"]), "sem frete e impostos", PURPLE_LIGHT),
                    kpi_card("Itens Vendidos",     fmt_number(kpi["total_items_sold"]),        "",                     CYAN_LIGHT),
                    kpi_card("Pedidos Online",     "87,9 %",                                   "do volume total",      GREEN),
                    kpi_card("Países Atendidos",   "6",                                        "mercados ativos",      AMBER),
                ], className="g-3 mb-5"),

                # ── Tendência & Canal ─────────────────────────────────────────
                section_title("Tendência Temporal e Canal de Vendas"),
                dbc.Row([
                    dbc.Col(chart_card(fig_monthly_revenue(), height=370), md=8),
                    dbc.Col(chart_card(fig_sales_channel(), height=370),   md=4),
                ], className="g-3 mb-4"),

                # ── Produtos & Categorias ─────────────────────────────────────
                section_title("Análise de Produto"),
                dbc.Row([
                    dbc.Col(chart_card(fig_top_products(), height=380),       md=6),
                    dbc.Col(chart_card(fig_revenue_by_category(), height=380), md=6),
                ], className="g-3 mb-4"),

                # ── Geográfico & Força de Vendas ──────────────────────────────
                section_title("Análise Geográfica e Força de Vendas"),
                dbc.Row([
                    dbc.Col(chart_card(fig_revenue_by_country(), height=360),  md=6),
                    dbc.Col(chart_card(fig_sellers_by_region(), height=360),   md=6),
                ], className="g-3 mb-4"),

                # ── Comportamento & Status ────────────────────────────────────
                section_title("Comportamento de Compra e Status"),
                dbc.Row([
                    dbc.Col(chart_card(fig_sales_reasons(), height=360), md=6),
                    dbc.Col(chart_card(fig_order_status(), height=360),  md=6),
                ], className="g-3 mb-5"),

                # ── Rodapé ────────────────────────────────────────────────────
                html.Hr(style={"borderColor": CARD_BORDER}),
                html.P(
                    "Dados: AdventureWorks 2014  ·  Pipeline: Snowflake → dbt → Airflow  ·  Dashboard: Plotly Dash",
                    style={"textAlign": "center", "color": TEXT_MUTED,
                           "fontSize": "0.72rem", "paddingBottom": "32px"},
                ),
            ],
            fluid=True,
            style={"padding": "0 24px"},
        ),
    ],
)

# ── CSS Embutido ───────────────────────────────────────────────────────────────
app.index_string = app.index_string.replace(
    "</head>",
    f"""<style>
body {{ background-color: {BG}; }}
.kpi-card {{
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 12px;
    padding: 20px 24px;
    transition: border-color .2s, transform .15s;
}}
.kpi-card:hover {{
    border-color: {PURPLE};
    transform: translateY(-2px);
}}
.kpi-label {{
    color: {TEXT_MUTED};
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 6px;
    font-weight: 500;
}}
.kpi-value {{
    font-size: 1.75rem;
    font-weight: 700;
    margin: 0;
    line-height: 1.1;
}}
.kpi-sub {{
    color: {TEXT_MUTED};
    font-size: 0.71rem;
    margin-top: 4px;
    margin-bottom: 0;
}}
.chart-card {{
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 12px;
    padding: 8px;
    transition: border-color .2s;
}}
.chart-card:hover {{ border-color: {PURPLE}; }}
.js-plotly-plot .plotly .modebar {{ display: none !important; }}
</style>
</head>""",
)

if __name__ == "__main__":
    app.run(debug=False, port=8050)
