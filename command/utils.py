from rich.table import Table 
from rich import box 


def get_rich_table() -> Table:
    return Table(
        show_header=True, 
        header_style='bright_green',
        box=box.DOUBLE_EDGE
    )