import './App.css'
import 'bootstrap/dist/css/bootstrap.min.css';
import { useState } from 'react';
import { Button, Container, Alert } from 'react-bootstrap'

function displayError(err) {
    if (err.message === undefined) {
        return <></>
    }

    return <>
        <Alert className='alert-danger'>{err.message}</Alert>
    </>
}

function App() {
    const [err, SetErr] = useState({});

    async function Search() {
        SetErr({message: "Not implemented"})
    }

    return (
        <>
            <Container className='m-5 w-50 mx-auto text-center border p-5'>

                <h1>Search Engine 🔍</h1>
                {displayError(err)}
                <hr />

                <div className='d-flex p-2'>
                    <input type="text" className='form-control m-1' placeholder='Keywords here' />
                    <Button onClick={Search} className='btn btn-info m-1'>Search</Button>
                </div>

            </Container>
        </>
    )
}

export default App
