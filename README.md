<table>
  <tr>
    <td width="220" align="center">
      <picture>
        <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.png">
        <source media="(prefers-color-scheme: light)" srcset="assets/logo-light.png">
        <img alt="unict logo" src="assets/logo-light.png" width="200">
      </picture>
    </td>
    <td>
      <h1>Technologies for Advanced Programming</h1>

![Course](https://img.shields.io/badge/Course-TAP%202025%2F26-green)
![University](https://img.shields.io/badge/University-UNICT-orange)
![GitHub repo size](https://img.shields.io/github/repo-size/tapunict/tap2026)
    </td>
  </tr>
</table>

## Getting started
To download the course material, you can either **clone the repository** or **download the ZIP** file from the course page.
```bash
git clone https://github.com/tapunict/tap2026.git
```

## Notebook
All the Jupyter notebooks used in the lectures are stored in the `/doc` folder as `.ipynb` files.
To view them interactively, you’ll need to install [Quarto](https://quarto.org/docs/get-started/) and run:

```bash
quarto preview doc/nome_file.ipynb
```
This will produce an output similar to:
```text
Browse at http://localhost:7887/
Listening on http://127.0.0.1:7887/
```
Your default browser will automatically open an interactive preview of the notebook.

You can also specify a custom port using the `--port <numero_porta>` option.

## Exporting Slides
Alternatively, you can render each notebook as an HTML slide deck by running:
```bash
quarto render doc/nome_file.ipynb
```
The generated HTML files can then be opened in any modern web browser.

## Additional Notes
- Make sure you have Quarto and Jupyter properly installed before running any of the above commands.
- For more details and troubleshooting, refer to the official [Quarto documentation](https://quarto.org/docs/get-started/).
