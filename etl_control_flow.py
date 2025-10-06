from dw import DW
import Proyecto_BDA.extract as extract
import Proyecto_BDA.transform as transform
import Proyecto_BDA.load as load


if __name__ == '__main__':
    dw = DW(create=True)

    # TODO: Write the control flow
    load.XXX(dw,
        transform.XXX(
            extract.XXX()
        )
    )

    dw.close()
